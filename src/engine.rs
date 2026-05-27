use std::{error::Error, fmt, sync::Arc, time::Instant};

use arrow_json::ArrayWriter;
use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::DataType, record_batch::RecordBatch},
    dataframe::DataFrame,
    error::DataFusionError,
    execution::context::SQLOptions,
    prelude::{SessionConfig, SessionContext},
    sql::parser::Statement as DataFusionStatement,
};
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult, path::Path as ObjectPath,
};
use serde::{Serialize, Serializer, ser::SerializeStruct};
use sqlparser::{
    ast::{
        AccessExpr, BinaryOperator, CaseWhen, DateTimeField, DuplicateTreatment, Expr, Function,
        FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, GroupByExpr, Ident,
        ObjectName, ObjectNamePart, OrderBy, OrderByKind, Query, Select, SelectItem, SetExpr,
        Statement as SqlStatement, Subscript, UnaryOperator, Value,
        helpers::attached_token::AttachedToken,
    },
    dialect::GenericDialect,
    parser::Parser,
};
use tokio::time::timeout;
use tracing::{Instrument, field};
use uuid::Uuid;

use crate::{
    config::Config,
    error::{HarborError, Result},
    observability,
    table_cache::TableCache,
    udf,
    unity::UnityCatalogClient,
};

mod catalog;
mod metadata;
mod results;

use catalog::{
    DeltaTableOpener, ObjectStoreRoute, ObjectStoreRouteRegistry, TableOpener, UnityCatalog,
    UnityCatalogProviderList,
};
#[cfg(test)]
use catalog::{load_cached_table, table_object_store_route};
use results::{ResultLimits, materialize_stream};

#[derive(Clone)]
pub struct QueryEngine {
    config: Config,
    unity: Arc<dyn UnityCatalog>,
    table_opener: Arc<dyn TableOpener>,
    table_cache: TableCache,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GetColumnsMetadataRequest<'a> {
    pub(crate) catalog: Option<&'a str>,
    pub(crate) schema: Option<&'a str>,
    pub(crate) table: Option<&'a str>,
    pub(crate) column: Option<&'a str>,
}

impl QueryEngine {
    pub fn new(config: Config) -> Self {
        let unity = Arc::new(UnityCatalogClient::new(
            config.databricks_host.clone(),
            config.unity_request_timeout,
        ));
        Self::with_dependencies(config, unity, Arc::new(DeltaTableOpener))
    }

    fn with_dependencies(
        config: Config,
        unity: Arc<dyn UnityCatalog>,
        table_opener: Arc<dyn TableOpener>,
    ) -> Self {
        let table_cache = TableCache::new(config.table_cache_max_entries, config.table_cache_ttl);
        Self {
            unity,
            table_opener,
            table_cache,
            config,
        }
    }

    pub async fn execute(
        &self,
        bearer_token: &str,
        sql: &str,
        default_catalog: &str,
        default_schema: &str,
    ) -> Result<QueryResult> {
        self.execute_with_query_id(None, bearer_token, sql, default_catalog, default_schema)
            .await
    }

    pub(crate) async fn get_columns_metadata(
        &self,
        bearer_token: &str,
        request: GetColumnsMetadataRequest<'_>,
        default_catalog: &str,
        default_schema: &str,
    ) -> Result<QueryResult> {
        metadata::get_columns(
            self.unity.as_ref(),
            bearer_token,
            metadata::GetColumnsRequest {
                catalog: request.catalog.map(str::to_string),
                schema: request.schema.map(str::to_string),
                table: request.table.map(str::to_string),
                column: request.column.map(str::to_string),
            },
            default_catalog,
            default_schema,
        )
        .await
    }

    pub async fn execute_with_query_id(
        &self,
        query_id: Option<&str>,
        bearer_token: &str,
        sql: &str,
        default_catalog: &str,
        default_schema: &str,
    ) -> Result<QueryResult> {
        let generated_query_id;
        let query_id = match query_id {
            Some(query_id) => query_id,
            None => {
                generated_query_id = Uuid::new_v4().to_string();
                &generated_query_id
            }
        };
        let sql_observation = observability::get().sql_observation(sql);
        let catalog_hash = observability::stable_hash(default_catalog);
        let schema_hash = observability::stable_hash(default_schema);
        let span = tracing::info_span!(
            "query",
            query_id = %query_id,
            catalog_hash = %catalog_hash,
            schema_hash = %schema_hash,
            sql_hash = %sql_observation.hash,
            sql_len = sql_observation.len,
            sql = field::Empty,
        );
        if let Some(sql) = sql_observation.text.as_deref() {
            span.record("sql", field::display(sql));
        }

        observability::get()
            .metrics()
            .increment("harborsql_queries_started_total");
        let started = Instant::now();
        let timeout_result = timeout(
            self.config.query_timeout,
            self.execute_inner(bearer_token, sql, default_catalog, default_schema),
        )
        .instrument(span)
        .await
        .unwrap_or_else(|_| {
            Err(HarborError::Query(format!(
                "query exceeded HARBORSQL_QUERY_TIMEOUT_SECONDS={}",
                self.config.query_timeout.as_secs()
            )))
        });

        let duration = started.elapsed();
        observability::get()
            .metrics()
            .observe_duration("query_total", duration);
        match &timeout_result {
            Ok(result) => {
                observability::get()
                    .metrics()
                    .increment("harborsql_queries_succeeded_total");
                tracing::info!(
                    duration_ms = duration.as_millis() as u64,
                    row_count = result.row_count,
                    "query completed"
                );
            }
            Err(error) => {
                observability::get()
                    .metrics()
                    .increment("harborsql_queries_failed_total");
                let client_error = error.client_error();
                tracing::warn!(
                    duration_ms = duration.as_millis() as u64,
                    error_code = client_error.code,
                    internal_error = %error.redacted_internal_message(),
                    "query failed"
                );
            }
        }

        timeout_result
    }

    async fn execute_inner(
        &self,
        bearer_token: &str,
        sql: &str,
        default_catalog: &str,
        default_schema: &str,
    ) -> Result<QueryResult> {
        if let Some(result) = metadata::execute_show_statement(
            self.unity.as_ref(),
            bearer_token,
            sql,
            default_catalog,
            default_schema,
        )
        .await?
        {
            return Ok(result);
        }

        let session_config = SessionConfig::new()
            .with_default_catalog_and_schema(default_catalog, default_schema)
            .with_target_partitions(self.config.target_partitions)
            .set_bool("datafusion.sql_parser.enable_ident_normalization", false)
            .set_bool(
                "datafusion.execution.parquet.pushdown_filters",
                self.config.parquet_pushdown_filters,
            )
            .set_bool(
                "datafusion.execution.parquet.reorder_filters",
                self.config.parquet_reorder_filters,
            )
            .set_usize(
                "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
                self.config.skip_partial_aggregation_probe_rows_threshold,
            )
            .set_str(
                "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                &self
                    .config
                    .skip_partial_aggregation_probe_ratio_threshold
                    .to_string(),
            );
        let ctx = SessionContext::new_with_config(session_config);
        udf::register_udfs(&ctx);

        let object_store_routes = ObjectStoreRouteRegistry::default();
        ctx.register_catalog_list(Arc::new(UnityCatalogProviderList::new(
            self.unity.clone(),
            self.table_opener.clone(),
            self.config.clone(),
            bearer_token,
            self.table_cache.clone(),
            object_store_routes.clone(),
        )));

        let execution_sql = rewrite_sql_fast_paths_with_options(
            sql,
            RewriteOptions {
                databricks_count_star_alias_rewrite: self
                    .config
                    .databricks_count_star_alias_rewrite,
                databricks_expression_alias_rewrite: self
                    .config
                    .databricks_expression_alias_rewrite,
            },
        );
        let plan_started = Instant::now();
        let dataframe = plan_sql(&ctx, &execution_sql)
            .instrument(tracing::info_span!("datafusion_planning"))
            .await?;
        observability::get()
            .metrics()
            .observe_duration("datafusion_planning", plan_started.elapsed());
        for (object_store_url, routes) in object_store_routes.routes()? {
            ctx.register_object_store(
                &object_store_url,
                Arc::new(PrefixRoutingObjectStore::new(routes)),
            );
        }
        let execution_started = Instant::now();
        let stream = dataframe
            .execute_stream()
            .instrument(tracing::info_span!("datafusion_execution"))
            .await?;
        observability::get()
            .metrics()
            .observe_duration("datafusion_execute_stream", execution_started.elapsed());
        materialize_stream(
            stream,
            ResultLimits {
                max_rows: self.config.max_result_rows,
                max_bytes: self.config.max_result_bytes,
            },
        )
        .instrument(tracing::info_span!("result_materialization"))
        .await
    }
}

async fn plan_sql(ctx: &SessionContext, sql: &str) -> Result<DataFrame> {
    validate_read_only_statement(ctx, sql)?;
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    ctx.sql_with_options(sql, options)
        .await
        .map_err(harbor_error_from_datafusion)
}

fn validate_read_only_statement(ctx: &SessionContext, sql: &str) -> Result<()> {
    let state = ctx.state();
    let dialect = state.config_options().sql_parser.dialect;
    let statement = state
        .sql_to_statement(sql, &dialect)
        .map_err(harbor_error_from_datafusion)?;
    if is_read_only_datafusion_statement(&statement) {
        Ok(())
    } else {
        Err(HarborError::UnsupportedSql(
            "only read-only SELECT queries are supported".into(),
        ))
    }
}

fn harbor_error_from_datafusion(error: DataFusionError) -> HarborError {
    harbor_error_source(&error).unwrap_or(HarborError::DataFusion(error))
}

fn harbor_error_source(error: &DataFusionError) -> Option<HarborError> {
    let mut source = error.source();
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<HarborError>() {
            return Some(clone_harbor_error(error));
        }
        source = current.source();
    }
    None
}

fn clone_harbor_error(error: &HarborError) -> HarborError {
    match error {
        HarborError::Config(message) => HarborError::Config(message.clone()),
        HarborError::MissingBearerToken => HarborError::MissingBearerToken,
        HarborError::UnsupportedSql(message) => HarborError::UnsupportedSql(message.clone()),
        HarborError::Unity(message) => HarborError::Unity(message.clone()),
        HarborError::Query(message) => HarborError::Query(message.clone()),
        HarborError::UnsupportedResultType(message) => {
            HarborError::UnsupportedResultType(message.clone())
        }
        HarborError::Thrift(message) => HarborError::Thrift(message.clone()),
        HarborError::Http(_)
        | HarborError::Url(_)
        | HarborError::Delta(_)
        | HarborError::DataFusion(_)
        | HarborError::ArrowJson(_)
        | HarborError::Json(_)
        | HarborError::Logger(_)
        | HarborError::Io(_) => HarborError::Query(error.to_string()),
    }
}

struct PrefixRoutingObjectStore {
    routes: Vec<ObjectStoreRoute>,
}

impl PrefixRoutingObjectStore {
    fn new(mut routes: Vec<ObjectStoreRoute>) -> Self {
        routes.sort_by_key(|route| std::cmp::Reverse(route.prefix.len()));
        Self { routes }
    }

    fn store_for_location(&self, location: &ObjectPath) -> ObjectStoreResult<Arc<dyn ObjectStore>> {
        route_store_for_location(&self.routes, location)
    }

    fn store_for_prefix(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<Arc<dyn ObjectStore>> {
        let Some(prefix) = prefix else {
            return self.first_store("root list");
        };

        if let Ok(store) = self.store_for_location(prefix) {
            return Ok(store);
        }

        let prefix = prefix.as_ref();
        self.routes
            .iter()
            .find(|route| path_has_prefix(&route.prefix, prefix))
            .map(|route| route.store.clone())
            .ok_or_else(|| no_route_error(prefix))
    }

    fn first_store(&self, operation: &'static str) -> ObjectStoreResult<Arc<dyn ObjectStore>> {
        self.routes
            .first()
            .map(|route| route.store.clone())
            .ok_or_else(|| object_store::Error::Generic {
                store: "prefix-routing",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("no object stores registered for {operation}"),
                )),
            })
    }
}

impl fmt::Debug for PrefixRoutingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrefixRoutingObjectStore")
            .field(
                "prefixes",
                &self
                    .routes
                    .iter()
                    .map(|route| route.prefix.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl fmt::Display for PrefixRoutingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "PrefixRoutingObjectStore({} route(s))",
            self.routes.len()
        )
    }
}

#[async_trait]
impl ObjectStore for PrefixRoutingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.store_for_location(location)?
            .put_opts(location, payload, options)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.store_for_location(location)?
            .put_multipart_opts(location, options)
            .await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.store_for_location(location)?
            .get_opts(location, options)
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        let routes = Arc::new(self.routes.clone());
        locations
            .map(move |location| {
                let routes = routes.clone();
                async move {
                    let location = location?;
                    let store = route_store_for_location(routes.as_slice(), &location)?;
                    store.delete(&location).await?;
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        match self.store_for_prefix(prefix) {
            Ok(store) => store.list(prefix),
            Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        match self.store_for_prefix(prefix) {
            Ok(store) => store.list_with_offset(prefix, offset),
            Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        self.store_for_prefix(prefix)?
            .list_with_delimiter(prefix)
            .await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        let from_store = self.store_for_location(from)?;
        let to_store = self.store_for_location(to)?;
        if !Arc::ptr_eq(&from_store, &to_store) {
            return Err(object_store::Error::NotSupported {
                source: Box::new(std::io::Error::other(
                    "cross-prefix copy is not supported by prefix routing object store",
                )),
            });
        }

        from_store.copy_opts(from, to, options).await
    }
}

fn route_store_for_location(
    routes: &[ObjectStoreRoute],
    location: &ObjectPath,
) -> ObjectStoreResult<Arc<dyn ObjectStore>> {
    let location = location.as_ref();
    routes
        .iter()
        .find(|route| path_has_prefix(location, &route.prefix))
        .map(|route| route.store.clone())
        .ok_or_else(|| no_route_error(location))
}

fn path_has_prefix(path: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn no_route_error(location: &str) -> object_store::Error {
    object_store::Error::PermissionDenied {
        path: location.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("no Unity table credential route matched object path {location}"),
        )),
    }
}

#[derive(Clone, Copy, Debug)]
struct RewriteOptions {
    databricks_count_star_alias_rewrite: bool,
    databricks_expression_alias_rewrite: bool,
}

impl Default for RewriteOptions {
    fn default() -> Self {
        Self {
            databricks_count_star_alias_rewrite: true,
            databricks_expression_alias_rewrite: true,
        }
    }
}

#[cfg(test)]
fn rewrite_sql_fast_paths(sql: &str) -> String {
    rewrite_sql_fast_paths_with_options(sql, RewriteOptions::default())
}

fn rewrite_sql_fast_paths_with_options(sql: &str, options: RewriteOptions) -> String {
    let dialect = GenericDialect {};
    let Ok(mut statements) = Parser::parse_sql(&dialect, sql) else {
        return sql.to_string();
    };
    if statements.len() != 1 {
        return sql.to_string();
    }
    let mut changed = false;

    if let Some(statement) = statements.first_mut() {
        changed = rewrite_statement_fast_paths(statement, options);
    }

    if changed {
        statements[0].to_string()
    } else {
        sql.to_string()
    }
}

fn is_read_only_datafusion_statement(statement: &DataFusionStatement) -> bool {
    match statement {
        DataFusionStatement::Statement(statement) => is_read_only_query_statement(statement),
        DataFusionStatement::Explain(explain) => {
            is_read_only_datafusion_statement(&explain.statement)
        }
        DataFusionStatement::CreateExternalTable(_)
        | DataFusionStatement::CopyTo(_)
        | DataFusionStatement::Reset(_) => false,
    }
}

fn is_read_only_query_statement(statement: &SqlStatement) -> bool {
    match statement {
        SqlStatement::Query(_) => true,
        SqlStatement::Explain { statement, .. } => is_read_only_query_statement(statement),
        _ => false,
    }
}

fn rewrite_statement_fast_paths(statement: &mut SqlStatement, options: RewriteOptions) -> bool {
    match statement {
        SqlStatement::Query(query) => rewrite_query_fast_paths(query, options),
        SqlStatement::Explain { statement, .. } => rewrite_statement_fast_paths(statement, options),
        _ => false,
    }
}

fn rewrite_query_fast_paths(query: &mut Query, options: RewriteOptions) -> bool {
    let mut changed = false;
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            changed |= rewrite_query_fast_paths(&mut cte.query, options);
        }
    }
    changed |= match query.body.as_mut() {
        SetExpr::Select(select) => rewrite_select_fast_paths(select, options),
        SetExpr::Query(query) => rewrite_query_fast_paths(query, options),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr_fast_paths(left, options) | rewrite_set_expr_fast_paths(right, options)
        }
        _ => false,
    };
    if let Some(order_by) = &mut query.order_by {
        changed |= rewrite_order_by_fast_paths(order_by, options);
    }
    changed
}

fn rewrite_set_expr_fast_paths(set_expr: &mut SetExpr, options: RewriteOptions) -> bool {
    match set_expr {
        SetExpr::Select(select) => rewrite_select_fast_paths(select, options),
        SetExpr::Query(query) => rewrite_query_fast_paths(query, options),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr_fast_paths(left, options) | rewrite_set_expr_fast_paths(right, options)
        }
        _ => false,
    }
}

fn rewrite_order_by_fast_paths(order_by: &mut OrderBy, options: RewriteOptions) -> bool {
    if let OrderByKind::Expressions(expressions) = &mut order_by.kind {
        expressions.iter_mut().fold(false, |changed, expression| {
            changed | rewrite_expr_fast_paths(&mut expression.expr, options)
        })
    } else {
        false
    }
}

fn rewrite_select_fast_paths(select: &mut Select, options: RewriteOptions) -> bool {
    let mut changed = false;
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                changed |= rewrite_expr_fast_paths(expr, options);
                if let Some(alias) = databricks_projection_alias(expr, options) {
                    let aliased_expr = expr.clone();
                    *item = SelectItem::ExprWithAlias {
                        expr: aliased_expr,
                        alias: Ident::with_quote('"', alias),
                    };
                    changed = true;
                }
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                changed |= rewrite_expr_fast_paths(expr, options);
            }
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => continue,
        }
    }
    if let Some(selection) = &mut select.selection {
        changed |= rewrite_expr_fast_paths(selection, options);
    }
    if let Some(prewhere) = &mut select.prewhere {
        changed |= rewrite_expr_fast_paths(prewhere, options);
    }
    if let GroupByExpr::Expressions(expressions, _) = &mut select.group_by {
        for expression in &mut *expressions {
            changed |= rewrite_expr_fast_paths(expression, options);
        }
    }
    for expression in &mut select.cluster_by {
        changed |= rewrite_expr_fast_paths(expression, options);
    }
    for expression in &mut select.distribute_by {
        changed |= rewrite_expr_fast_paths(expression, options);
    }
    for order_by in &mut select.sort_by {
        changed |= rewrite_expr_fast_paths(&mut order_by.expr, options);
    }
    if let Some(having) = &mut select.having {
        changed |= rewrite_expr_fast_paths(having, options);
    }
    if let Some(qualify) = &mut select.qualify {
        changed |= rewrite_expr_fast_paths(qualify, options);
    }
    changed
}

fn rewrite_expr_fast_paths(expr: &mut Expr, options: RewriteOptions) -> bool {
    if rewrite_leaf_expr_fast_paths(expr, options) {
        return true;
    }

    match expr {
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            rewrite_expr_fast_paths(left, options) | rewrite_expr_fast_paths(right, options)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Nested(expr)
        | Expr::OuterJoin(expr)
        | Expr::Prior(expr) => rewrite_expr_fast_paths(expr, options),
        Expr::CompoundFieldAccess { root, access_chain } => {
            let root_is_databricks_get = is_databricks_get_array_expr(root);
            let mut changed = rewrite_expr_fast_paths(root, options);
            if root_is_databricks_get {
                changed |= rewrite_dot_accesses_as_named_field_subscripts(access_chain);
            }
            for access in access_chain {
                changed |= rewrite_access_expr_fast_paths(access, options);
            }
            changed
        }
        _ => false,
    }
}

fn rewrite_leaf_expr_fast_paths(expr: &mut Expr, options: RewriteOptions) -> bool {
    match expr {
        Expr::Extract {
            field: DateTimeField::Minute | DateTimeField::Minutes,
            expr: source,
            ..
        } => {
            *expr = extract_minute_expr((**source).clone());
            true
        }
        Expr::Function(function) => {
            let changed_args = rewrite_function_fast_paths(function, options);
            if let Some((array, index)) = databricks_get_array_args(function) {
                *expr = databricks_get_array_expr(array, index);
                true
            } else if let Some((map, key)) = databricks_element_at_map_args(function) {
                *expr = databricks_element_at_map_expr(map, key);
                true
            } else if let Some((source, pattern, capture_index)) =
                regexp_replace_capture_fast_path_args(function)
            {
                *expr = regexp_replace_capture_expr(source, pattern, capture_index);
                true
            } else {
                changed_args
            }
        }
        Expr::Like {
            negated,
            any: false,
            expr: like_expr,
            pattern,
            escape_char: None,
        } => {
            let Some(needle) = contains_needle_from_like_pattern(pattern) else {
                return false;
            };
            *expr = contains_expr((**like_expr).clone(), needle, *negated);
            true
        }
        _ => false,
    }
}

fn rewrite_access_expr_fast_paths(access: &mut AccessExpr, options: RewriteOptions) -> bool {
    match access {
        AccessExpr::Dot(expr) => rewrite_expr_fast_paths(expr, options),
        AccessExpr::Subscript(subscript) => rewrite_subscript_fast_paths(subscript, options),
    }
}

fn rewrite_subscript_fast_paths(subscript: &mut Subscript, options: RewriteOptions) -> bool {
    match subscript {
        Subscript::Index { index } => rewrite_expr_fast_paths(index, options),
        Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        } => {
            let mut changed = false;
            if let Some(lower_bound) = lower_bound {
                changed |= rewrite_expr_fast_paths(lower_bound, options);
            }
            if let Some(upper_bound) = upper_bound {
                changed |= rewrite_expr_fast_paths(upper_bound, options);
            }
            if let Some(stride) = stride {
                changed |= rewrite_expr_fast_paths(stride, options);
            }
            changed
        }
    }
}

fn is_databricks_get_array_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Function(function) if databricks_get_array_args(function).is_some())
}

fn rewrite_dot_accesses_as_named_field_subscripts(access_chain: &mut [AccessExpr]) -> bool {
    let mut changed = false;
    for access in access_chain {
        if let AccessExpr::Dot(Expr::Identifier(ident)) = access {
            let field_name = ident.value.clone();
            *access = AccessExpr::Subscript(Subscript::Index {
                index: single_quoted_string_expr(field_name),
            });
            changed = true;
        }
    }
    changed
}

fn extract_minute_expr(source: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(Ident::new(udf::EXTRACT_MINUTE_UDF)),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(source))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn rewrite_function_fast_paths(function: &mut Function, options: RewriteOptions) -> bool {
    let mut changed = false;
    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = &mut function.args {
        for arg in args {
            changed |= rewrite_function_arg_fast_paths(arg, options);
        }
    }
    changed
}

fn rewrite_function_arg_fast_paths(arg: &mut FunctionArg, options: RewriteOptions) -> bool {
    match arg {
        FunctionArg::Named { arg, .. }
        | FunctionArg::ExprNamed { arg, .. }
        | FunctionArg::Unnamed(arg) => match arg {
            FunctionArgExpr::Expr(expr) => rewrite_expr_fast_paths(expr, options),
            FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => false,
        },
    }
}

fn is_count_wildcard_projection(expr: &Expr) -> bool {
    match expr {
        Expr::Function(function) => is_count_wildcard_function(function),
        Expr::Nested(expr) => is_count_wildcard_projection(expr),
        _ => false,
    }
}

fn databricks_projection_alias(expr: &Expr, options: RewriteOptions) -> Option<String> {
    if options.databricks_count_star_alias_rewrite && is_count_wildcard_projection(expr) {
        return Some("count(1)".to_string());
    }
    if options.databricks_expression_alias_rewrite && should_alias_databricks_expression(expr) {
        return Some(databricks_expr_name(expr));
    }
    None
}

fn should_alias_databricks_expression(expr: &Expr) -> bool {
    match expr {
        Expr::Function(_) | Expr::BinaryOp { .. } | Expr::Value(_) => true,
        Expr::Nested(expr) => should_alias_databricks_expression(expr),
        _ => false,
    }
}

fn databricks_expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|ident| ident.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        Expr::Value(value) => databricks_value_name(&value.value),
        Expr::BinaryOp { left, op, right } => {
            format!(
                "({} {} {})",
                databricks_expr_name(left),
                databricks_binary_operator_name(op),
                databricks_expr_name(right)
            )
        }
        Expr::Function(function) => databricks_function_name(function),
        Expr::Nested(expr) => format!("({})", databricks_expr_name(expr)),
        Expr::UnaryOp { op, expr } => format!("{op}{}", databricks_expr_name(expr)),
        _ => expr.to_string(),
    }
}

fn databricks_value_name(value: &Value) -> String {
    match value {
        Value::Number(value, _) => value.clone(),
        _ => value.to_string(),
    }
}

fn databricks_binary_operator_name(op: &BinaryOperator) -> String {
    match op {
        BinaryOperator::Plus => "+".to_string(),
        BinaryOperator::Minus => "-".to_string(),
        BinaryOperator::Multiply => "*".to_string(),
        BinaryOperator::Divide => "/".to_string(),
        BinaryOperator::Modulo => "%".to_string(),
        _ => op.to_string(),
    }
}

fn databricks_function_name(function: &Function) -> String {
    if let Some(args) = databricks_function_argument_list_name(function) {
        format!("{}({args})", function.name.to_string().to_ascii_lowercase())
    } else {
        function.to_string()
    }
}

fn databricks_function_argument_list_name(function: &Function) -> Option<String> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }

    let FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment,
        args,
        clauses,
    }) = &function.args
    else {
        return None;
    };
    if !clauses.is_empty() {
        return None;
    }

    let mut rendered_args = String::new();
    if let Some(duplicate_treatment) = duplicate_treatment {
        match duplicate_treatment {
            DuplicateTreatment::Distinct => rendered_args.push_str("DISTINCT "),
            DuplicateTreatment::All => rendered_args.push_str("ALL "),
        }
    }
    rendered_args.push_str(
        &args
            .iter()
            .map(databricks_function_arg_name)
            .collect::<Vec<_>>()
            .join(", "),
    );
    Some(rendered_args)
}

fn databricks_function_arg_name(arg: &FunctionArg) -> String {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => databricks_expr_name(expr),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => "*".to_string(),
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name)) => format!("{name}.*"),
        _ => arg.to_string(),
    }
}

fn is_count_wildcard_function(function: &Function) -> bool {
    if !function_name_eq(function, "count")
        || function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return false;
    }

    let FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: None,
        args,
        clauses,
    }) = &function.args
    else {
        return false;
    };

    clauses.is_empty()
        && args.len() == 1
        && matches!(
            args.first(),
            Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
        )
}

fn regexp_replace_capture_fast_path_args(function: &Function) -> Option<(Expr, String, u64)> {
    if !function_name_eq(function, "regexp_replace")
        || function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }

    let FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: None,
        args,
        clauses,
    }) = &function.args
    else {
        return None;
    };
    if args.len() != 3 || !clauses.is_empty() {
        return None;
    }

    let source = function_arg_expr(args.first()?)?.clone();
    let pattern = string_literal_value(function_arg_expr(args.get(1)?)?)?.to_string();
    let replacement = string_literal_value(function_arg_expr(args.get(2)?)?)?;
    let capture_index = capture_replacement_index(replacement)?;
    Some((source, pattern, capture_index))
}

fn databricks_get_array_args(function: &Function) -> Option<(Expr, Expr)> {
    if !function_name_eq(function, "get")
        || function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }

    let FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: None,
        args,
        clauses,
    }) = &function.args
    else {
        return None;
    };
    if args.len() != 2 || !clauses.is_empty() {
        return None;
    }

    let array = function_arg_expr(args.first()?)?.clone();
    let index = function_arg_expr(args.get(1)?)?.clone();
    Some((array, index))
}

fn databricks_element_at_map_args(function: &Function) -> Option<(Expr, Expr)> {
    if !function_name_eq(function, "element_at")
        || function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }

    let FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: None,
        args,
        clauses,
    }) = &function.args
    else {
        return None;
    };
    if args.len() != 2 || !clauses.is_empty() {
        return None;
    }

    let map = function_arg_expr(args.first()?)?.clone();
    let key = function_arg_expr(args.get(1)?)?.clone();
    Some((map, key))
}

fn function_name_eq(function: &Function, expected: &str) -> bool {
    if function.name.0.len() != 1 {
        return false;
    }
    match &function.name.0[0] {
        ObjectNamePart::Identifier(ident) => ident.value.eq_ignore_ascii_case(expected),
        _ => false,
    }
}

fn function_arg_expr(arg: &FunctionArg) -> Option<&Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
        _ => None,
    }
}

fn capture_replacement_index(replacement: &str) -> Option<u64> {
    let digits = replacement
        .strip_prefix('$')
        .or_else(|| replacement.strip_prefix('\\'))?;
    let digits = digits
        .strip_prefix('{')
        .and_then(|digits| digits.strip_suffix('}'))
        .unwrap_or(digits);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn databricks_get_array_expr(array: Expr, zero_based_index: Expr) -> Expr {
    array_element_expr(array, databricks_get_array_index_expr(zero_based_index))
}

fn databricks_element_at_map_expr(map: Expr, key: Expr) -> Expr {
    // DataFusion's map_extract/element_at returns a one-value list. Databricks
    // returns the extracted map value itself.
    array_element_expr(map_extract_expr(map, key), number_expr("1"))
}

fn array_element_expr(array: Expr, one_based_index: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(Ident::new("array_element")),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(array)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(one_based_index)),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn map_extract_expr(map: Expr, key: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(Ident::new("map_extract")),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(map)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(key)),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn databricks_get_array_index_expr(index: Expr) -> Expr {
    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen {
            condition: Expr::BinaryOp {
                left: Box::new(index.clone()),
                op: BinaryOperator::Lt,
                right: Box::new(number_expr("0")),
            },
            result: number_expr("0"),
        }],
        else_result: Some(Box::new(Expr::BinaryOp {
            left: Box::new(index),
            op: BinaryOperator::Plus,
            right: Box::new(number_expr("1")),
        })),
    }
}

fn number_expr(value: &str) -> Expr {
    Expr::Value(Value::Number(value.to_string(), false).into())
}

fn single_quoted_string_expr(value: String) -> Expr {
    Expr::Value(Value::SingleQuotedString(value).into())
}

fn regexp_replace_capture_expr(source: Expr, pattern: String, capture_index: u64) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(Ident::new(udf::REGEXP_REPLACE_CAPTURE_UDF)),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(source)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(pattern).into(),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::Number(capture_index.to_string(), false).into(),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn contains_expr(haystack: Expr, needle: String, negated: bool) -> Expr {
    let contains = Expr::Function(Function {
        name: ObjectName::from(Ident::new("contains")),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(haystack)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(needle).into(),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    });

    if negated {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(contains),
        }
    } else {
        contains
    }
}

fn contains_needle_from_like_pattern(pattern: &Expr) -> Option<String> {
    let pattern = string_literal_value(pattern)?;
    let needle = pattern.strip_prefix('%')?.strip_suffix('%')?;
    if needle.contains(['%', '_']) {
        return None;
    }
    Some(needle.to_string())
}

fn string_literal_value(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(value)
            | Value::EscapedStringLiteral(value)
            | Value::DoubleQuotedString(value) => Some(value),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Clone)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub row_count: usize,
    data_types: Vec<DataType>,
    store: Arc<dyn ResultStore>,
}

impl QueryResult {
    pub fn from_batches(columns: Vec<Column>, batches: Vec<RecordBatch>) -> Self {
        let data_types = infer_result_data_types(&columns, &batches);
        Self::from_batches_with_data_types(columns, data_types, batches)
    }

    pub fn from_batches_with_data_types(
        columns: Vec<Column>,
        data_types: Vec<DataType>,
        batches: Vec<RecordBatch>,
    ) -> Self {
        let store = Arc::new(InlineResultStore::new(batches));
        Self {
            columns,
            row_count: store.row_count(),
            data_types,
            store,
        }
    }

    pub fn empty() -> Self {
        Self::from_batches(Vec::new(), Vec::new())
    }

    pub fn page(&self, start_row_offset: i64, limit: usize) -> QueryResultPage {
        self.store.page(start_row_offset, limit)
    }

    pub fn data_types(&self) -> &[DataType] {
        &self.data_types
    }

    fn rows_json(&self) -> Result<serde_json::Value> {
        self.store.rows_json()
    }
}

impl fmt::Debug for QueryResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryResult")
            .field("columns", &self.columns)
            .field("data_types", &self.data_types)
            .field("row_count", &self.row_count)
            .field("store_kind", &self.store.kind())
            .field("retained_bytes", &self.store.retained_bytes())
            .finish()
    }
}

fn infer_result_data_types(columns: &[Column], batches: &[RecordBatch]) -> Vec<DataType> {
    if let Some(batch) = batches.first() {
        return batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
    }

    columns
        .iter()
        .map(|column| parse_column_data_type(&column.data_type).unwrap_or(DataType::Null))
        .collect()
}

fn parse_column_data_type(value: &str) -> Option<DataType> {
    match value.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => Some(DataType::Boolean),
        "int8" => Some(DataType::Int8),
        "int16" => Some(DataType::Int16),
        "int32" => Some(DataType::Int32),
        "int64" => Some(DataType::Int64),
        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),
        "float32" => Some(DataType::Float32),
        "float64" => Some(DataType::Float64),
        "utf8" => Some(DataType::Utf8),
        "largeutf8" | "large_utf8" => Some(DataType::LargeUtf8),
        "date32" | "date32[day]" => Some(DataType::Date32),
        "date64" | "date64[ms]" | "date64[millisecond]" => Some(DataType::Date64),
        _ => None,
    }
}

impl Serialize for QueryResult {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("QueryResult", 3)?;
        state.serialize_field("columns", &self.columns)?;
        state.serialize_field("rows", &JsonRows(self))?;
        state.serialize_field("row_count", &self.row_count)?;
        state.end()
    }
}

struct JsonRows<'a>(&'a QueryResult);

impl Serialize for JsonRows<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0
            .rows_json()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

#[derive(Debug, Clone)]
pub struct QueryResultPage {
    pub batches: Vec<RecordBatch>,
    pub start_row_offset: i64,
    pub row_count: usize,
    pub has_more_rows: bool,
}

trait ResultStore: Send + Sync {
    fn kind(&self) -> &'static str;
    fn row_count(&self) -> usize;
    fn retained_bytes(&self) -> usize;
    fn page(&self, start_row_offset: i64, limit: usize) -> QueryResultPage;
    fn rows_json(&self) -> Result<serde_json::Value>;
}

#[derive(Debug)]
struct InlineResultStore {
    batches: Vec<RecordBatch>,
    row_count: usize,
    retained_bytes: usize,
}

impl InlineResultStore {
    fn new(batches: Vec<RecordBatch>) -> Self {
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        let retained_bytes = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        Self {
            batches,
            row_count,
            retained_bytes,
        }
    }
}

impl ResultStore for InlineResultStore {
    fn kind(&self) -> &'static str {
        "inline-arrow"
    }

    fn row_count(&self) -> usize {
        self.row_count
    }

    fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn page(&self, start_row_offset: i64, limit: usize) -> QueryResultPage {
        let start = usize::try_from(start_row_offset.max(0)).unwrap_or(usize::MAX);
        let start = start.min(self.row_count);
        let end = start.saturating_add(limit).min(self.row_count);
        let mut remaining_skip = start;
        let mut remaining_take = end.saturating_sub(start);
        let mut page_batches = Vec::new();

        for batch in &self.batches {
            if remaining_take == 0 {
                break;
            }

            let batch_rows = batch.num_rows();
            if remaining_skip >= batch_rows {
                remaining_skip -= batch_rows;
                continue;
            }

            let local_start = remaining_skip;
            let local_take = (batch_rows - local_start).min(remaining_take);
            page_batches.push(batch.slice(local_start, local_take));
            remaining_take -= local_take;
            remaining_skip = 0;
        }

        QueryResultPage {
            batches: page_batches,
            start_row_offset: start as i64,
            row_count: end.saturating_sub(start),
            has_more_rows: end < self.row_count,
        }
    }

    fn rows_json(&self) -> Result<serde_json::Value> {
        let mut writer = ArrayWriter::new(Vec::new());
        for batch in &self.batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        Ok(serde_json::from_slice(&writer.into_inner())?)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use datafusion::{
        arrow::{
            array::{
                Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Builder,
                Int32Array, Int32Builder, ListArray, ListBuilder, MapArray, MapBuilder,
                StringArray, StringBuilder, StructArray, StructBuilder,
            },
            datatypes::{DataType, Field, Schema},
        },
        catalog::TableProvider,
        datasource::{MemTable, empty::EmptyTable},
        physical_plan::{SendableRecordBatchStream, memory::MemoryStream},
    };
    use object_store::memory::InMemory;
    use url::Url;

    use crate::{
        table_cache::CachedTable,
        unity::{
            AwsTempCredentials, CatalogInfo, ColumnInfo, SchemaInfo, TableInfo,
            TemporaryTableCredentials,
        },
    };

    use super::*;

    #[test]
    fn derives_bucket_route_from_table_storage_url() {
        let (object_store_url, prefix) =
            table_object_store_route("s3://bench-bucket/ssb/sf10/tables/lineorder").unwrap();

        assert_eq!(object_store_url.scheme(), "s3");
        assert_eq!(object_store_url.host_str(), Some("bench-bucket"));
        assert_eq!(object_store_url.path(), "");
        assert_eq!(prefix, "ssb/sf10/tables/lineorder");
    }

    #[test]
    fn route_prefixes_match_complete_path_segments() {
        assert!(path_has_prefix(
            "ssb/sf10/tables/lineorder/part-00000.parquet",
            "ssb/sf10/tables/lineorder"
        ));
        assert!(!path_has_prefix(
            "ssb/sf10/tables/lineorder_extra/part-00000.parquet",
            "ssb/sf10/tables/lineorder"
        ));
    }

    #[tokio::test]
    async fn routes_same_bucket_reads_to_the_matching_table_store() {
        use object_store::{ObjectStoreExt as _, memory::InMemory};

        let date_store = Arc::new(InMemory::new());
        let lineorder_store = Arc::new(InMemory::new());
        let date_path = ObjectPath::from("ssb/sf10/tables/date/part-00000.parquet");
        let lineorder_path = ObjectPath::from("ssb/sf10/tables/lineorder/part-00000.parquet");

        date_store.put(&date_path, "date".into()).await.unwrap();
        lineorder_store
            .put(&lineorder_path, "lineorder".into())
            .await
            .unwrap();

        let router = PrefixRoutingObjectStore::new(vec![
            ObjectStoreRoute {
                prefix: "ssb/sf10/tables/date".to_string(),
                store: date_store,
            },
            ObjectStoreRoute {
                prefix: "ssb/sf10/tables/lineorder".to_string(),
                store: lineorder_store,
            },
        ]);

        let date_bytes = router.get(&date_path).await.unwrap().bytes().await.unwrap();
        let lineorder_bytes = router
            .get(&lineorder_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(&date_bytes[..], b"date");
        assert_eq!(&lineorder_bytes[..], b"lineorder");
    }

    #[test]
    fn rewrites_simple_contains_like_predicates() {
        let rewritten = rewrite_sql_fast_paths(
            "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%' AND URL NOT LIKE '%.google.%'",
        );

        assert!(rewritten.contains(r#"COUNT(*) AS "count(1)""#));
        assert!(rewritten.contains("contains(URL, 'google')"));
        assert!(rewritten.contains("NOT contains(URL, '.google.')"));
        assert!(!rewritten.contains(" LIKE "));
    }

    #[test]
    fn aliases_unaliased_count_wildcard_projection_for_databricks_metadata() {
        let rewritten = rewrite_sql_fast_paths("SELECT COUNT(*) FROM hits");

        assert_eq!(rewritten, r#"SELECT COUNT(*) AS "count(1)" FROM hits"#);
    }

    #[test]
    fn preserves_explicit_count_wildcard_aliases() {
        let sql = "SELECT COUNT(*) AS c FROM hits";

        assert_eq!(rewrite_sql_fast_paths(sql), sql);
    }

    #[test]
    fn can_disable_databricks_count_wildcard_projection_alias_rewrite() {
        let sql = "SELECT COUNT(*) FROM hits";
        let rewritten = rewrite_sql_fast_paths_with_options(
            sql,
            RewriteOptions {
                databricks_count_star_alias_rewrite: false,
                databricks_expression_alias_rewrite: true,
            },
        );

        assert_eq!(rewritten, r#"SELECT COUNT(*) AS "count(*)" FROM hits"#);
    }

    #[test]
    fn aliases_unaliased_expression_projections_for_databricks_metadata() {
        let rewritten =
            rewrite_sql_fast_paths("SELECT 1, ClientIP - 1, SUM(ResolutionWidth + 2) FROM hits");

        assert_eq!(
            rewritten,
            r#"SELECT 1 AS "1", ClientIP - 1 AS "(ClientIP - 1)", SUM(ResolutionWidth + 2) AS "sum((ResolutionWidth + 2))" FROM hits"#
        );
    }

    #[test]
    fn preserves_explicit_expression_aliases() {
        let sql = "SELECT 1 AS one, SUM(ResolutionWidth + 2) AS total FROM hits";

        assert_eq!(rewrite_sql_fast_paths(sql), sql);
    }

    #[test]
    fn can_disable_databricks_expression_projection_alias_rewrite() {
        let sql = "SELECT 1, ClientIP - 1, SUM(ResolutionWidth + 2) FROM hits";
        let rewritten = rewrite_sql_fast_paths_with_options(
            sql,
            RewriteOptions {
                databricks_count_star_alias_rewrite: true,
                databricks_expression_alias_rewrite: false,
            },
        );

        assert_eq!(rewritten, sql);
    }

    #[test]
    fn rewrites_simple_contains_like_inside_explain() {
        let rewritten = rewrite_sql_fast_paths(
            "EXPLAIN ANALYZE SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
        );

        assert!(rewritten.contains("EXPLAIN ANALYZE SELECT"));
        assert!(rewritten.contains("contains(URL, 'google')"));
    }

    #[test]
    fn rewrites_single_capture_regexp_replace() {
        let rewritten = rewrite_sql_fast_paths(
            "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '$1') AS k FROM hits",
        );

        assert!(rewritten.contains("harborsql_regexp_replace_capture"));
        assert!(!rewritten.contains("'$1'"));
    }

    #[test]
    fn rewrites_single_capture_regexp_replace_in_group_by() {
        let rewritten = rewrite_sql_fast_paths(
            "SELECT COUNT(*) FROM hits GROUP BY REGEXP_REPLACE(Referer, '(.*)', '$1')",
        );

        assert!(rewritten.contains("GROUP BY harborsql_regexp_replace_capture"));
    }

    #[test]
    fn rewrites_extract_minute() {
        let rewritten = rewrite_sql_fast_paths(
            "SELECT UserID, extract(minute FROM EventTime) AS m FROM hits GROUP BY UserID, m",
        );

        assert!(rewritten.contains("harborsql_extract_minute(EventTime) AS m"));
    }

    #[test]
    fn rewrites_databricks_get_array_access() {
        let rewritten =
            rewrite_sql_fast_paths("SELECT get(items, 0).prices AS first_prices FROM hits");

        assert!(
            rewritten.contains(
                "array_element(items, CASE WHEN 0 < 0 THEN 0 ELSE 0 + 1 END)['prices'] AS first_prices"
            ),
            "{rewritten}"
        );
        assert!(!rewritten.to_ascii_lowercase().contains("get("));
    }

    #[test]
    fn rewrites_databricks_element_at_map_access() {
        let rewritten =
            rewrite_sql_fast_paths("SELECT element_at(attrs, 'one') AS map_one FROM hits");

        assert!(
            rewritten.contains("array_element(map_extract(attrs, 'one'), 1) AS map_one"),
            "{rewritten}"
        );
        assert!(!rewritten.to_ascii_lowercase().contains("element_at("));
    }

    #[test]
    fn leaves_complex_regexp_replace_unchanged() {
        let sql = "SELECT REGEXP_REPLACE(Referer, 'foo', 'bar') FROM hits";
        assert_eq!(
            rewrite_sql_fast_paths_with_options(
                sql,
                RewriteOptions {
                    databricks_count_star_alias_rewrite: true,
                    databricks_expression_alias_rewrite: false,
                },
            ),
            sql
        );
    }

    #[test]
    fn leaves_complex_like_predicates_unchanged() {
        let sql = "SELECT URL FROM hits WHERE URL LIKE '%goo_le%'";

        assert_eq!(rewrite_sql_fast_paths(sql), sql);
    }

    #[tokio::test]
    async fn load_cached_table_surfaces_unity_table_errors() {
        let err = expect_cached_table_error(
            load_cached_table(
                Arc::new(MockUnity::table_error("table unavailable")),
                Arc::new(MockTableOpener::ok()),
                test_config(),
                "token",
                "workspace.default.hits",
            )
            .await,
        );

        assert!(matches!(err, HarborError::Unity(message) if message == "table unavailable"));
    }

    #[tokio::test]
    async fn load_cached_table_surfaces_credential_errors() {
        let opener = Arc::new(MockTableOpener::ok());
        let err = expect_cached_table_error(
            load_cached_table(
                Arc::new(MockUnity::credential_error("credentials unavailable")),
                opener.clone(),
                test_config(),
                "token",
                "workspace.default.hits",
            )
            .await,
        );

        assert!(matches!(err, HarborError::Unity(message) if message == "credentials unavailable"));
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn load_cached_table_rejects_non_delta_tables_before_opening_storage() {
        let opener = Arc::new(MockTableOpener::ok());
        let err = expect_cached_table_error(
            load_cached_table(
                Arc::new(MockUnity::non_delta()),
                opener.clone(),
                test_config(),
                "token",
                "workspace.default.hits",
            )
            .await,
        );

        assert!(
            matches!(err, HarborError::UnsupportedSql(message) if message.contains("not an externally readable Delta table"))
        );
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn load_cached_table_surfaces_table_opener_errors() {
        let opener = Arc::new(MockTableOpener::error("delta open failed"));
        let err = expect_cached_table_error(
            load_cached_table(
                Arc::new(MockUnity::delta()),
                opener.clone(),
                test_config(),
                "token",
                "workspace.default.hits",
            )
            .await,
        );

        assert!(matches!(err, HarborError::Query(message) if message == "delta open failed"));
        assert_eq!(opener.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn execute_enforces_result_row_limits_with_injected_table_provider() {
        let mut config = test_config();
        config.max_result_rows = Some(1);
        let engine = QueryEngine::with_dependencies(
            config,
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![1, 2]),
            ))),
        );

        let err = engine
            .execute("token", "SELECT * FROM hits", "workspace", "default")
            .await
            .unwrap_err();

        assert!(
            matches!(err, HarborError::Query(message) if message.contains("HARBORSQL_MAX_RESULT_ROWS=1"))
        );
    }

    #[tokio::test]
    async fn execute_surfaces_datafusion_planning_failures() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![1]),
            ))),
        );

        let err = engine
            .execute("token", "SELECT missing FROM hits", "workspace", "default")
            .await
            .unwrap_err();

        assert!(matches!(err, HarborError::DataFusion(_)));
    }

    #[tokio::test]
    async fn execute_aliases_unaliased_count_wildcard_column_name_by_default() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![1, 2]),
            ))),
        );

        let result = engine
            .execute("token", "SELECT COUNT(*) FROM hits", "workspace", "default")
            .await
            .unwrap();

        assert_eq!(result.columns[0].name, "count(1)");
        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn execute_can_disable_count_wildcard_column_name_alias_rewrite() {
        let mut config = test_config();
        config.databricks_count_star_alias_rewrite = false;
        let engine = QueryEngine::with_dependencies(
            config,
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![1, 2]),
            ))),
        );

        let result = engine
            .execute("token", "SELECT COUNT(*) FROM hits", "workspace", "default")
            .await
            .unwrap();

        assert_eq!(result.columns[0].name, "count(*)");
        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn execute_aliases_unaliased_expression_column_names_by_default() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![1, 2]),
            ))),
        );

        let literal_result = engine
            .execute("token", "SELECT 1", "workspace", "default")
            .await
            .unwrap();
        assert_eq!(literal_result.columns[0].name, "1");

        let aggregate_result = engine
            .execute(
                "token",
                "SELECT SUM(id + 2) FROM hits",
                "workspace",
                "default",
            )
            .await
            .unwrap();
        assert_eq!(aggregate_result.columns[0].name, "sum((id + 2))");
    }

    #[tokio::test]
    async fn execute_can_disable_expression_column_name_alias_rewrite() {
        let mut config = test_config();
        config.databricks_expression_alias_rewrite = false;
        let engine = QueryEngine::with_dependencies(
            config,
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![1, 2]),
            ))),
        );

        let result = engine
            .execute("token", "SELECT 1", "workspace", "default")
            .await
            .unwrap();

        assert_eq!(result.columns[0].name, "Int64(1)");
    }

    #[tokio::test]
    async fn execute_surfaces_unity_errors_from_lazy_catalog_lookup() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::table_error("table unavailable")),
            Arc::new(MockTableOpener::ok()),
        );

        let err = engine
            .execute("token", "SELECT * FROM hits", "workspace", "default")
            .await
            .unwrap_err();

        assert!(matches!(err, HarborError::Unity(message) if message == "table unavailable"));
    }

    #[tokio::test]
    async fn execute_uses_datafusion_resolver_for_quoted_identifiers() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![7]),
            ))),
        );

        let result = engine
            .execute(
                "token",
                "SELECT id FROM \"Hits Table\"",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn execute_uses_datafusion_resolver_for_nested_ctes_and_scalar_subqueries() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![1, 2]),
            ))),
        );

        let result = engine
            .execute(
                "token",
                "WITH base AS (SELECT id FROM hits) \
                 SELECT id, (SELECT max(id) FROM scores) AS max_score FROM base",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 2);
    }

    #[tokio::test]
    async fn execute_handles_delta_type_nested_field_access_query_shape() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                delta_type_nested_access_batch(),
            ))),
        );

        let result = engine
            .execute(
                "token",
                "SELECT \
                    row_id, \
                    c_struct_scalar.name AS scalar_name, \
                    c_struct_nested.child.effective_date AS child_effective_date, \
                    element_at(c_map_string_int, 'one') AS map_one, \
                    element_at(c_map_string_array, 'small') AS map_array_small, \
                    get(c_struct_all_complex.items, 0).prices AS first_item_prices \
                 FROM hits \
                 ORDER BY row_id",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns.len(), 6);
        assert!(matches!(result.data_types()[3], DataType::Int32));
        assert!(matches!(result.data_types()[4], DataType::List(_)));
        assert!(matches!(result.data_types()[5], DataType::Map(_, _)));

        let page = result.page(0, 10);
        let batch = &page.batches[0];
        let map_one = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(map_one.value(0), 1);
        assert!(map_one.is_null(1));

        let map_array_small = batch
            .column(4)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let first_map_array_small = map_array_small.value(0);
        let first_map_array_small = first_map_array_small
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(first_map_array_small.values(), &[1, 2]);
        assert!(map_array_small.is_null(1));

        let first_item_prices = batch.column(5).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!first_item_prices.is_null(0));
        assert!(first_item_prices.is_null(1));
    }

    #[tokio::test]
    async fn execute_handles_databricks_length_on_binary_and_string_values() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                delta_type_scalar_length_batch(),
            ))),
        );

        let result = engine
            .execute(
                "token",
                "SELECT \
                    row_id, \
                    length(c_binary) AS c_binary_length, \
                    length(c_string) AS c_string_length \
                 FROM hits \
                 ORDER BY row_id",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 3);
        assert_eq!(result.columns.len(), 3);
        assert!(matches!(result.data_types()[1], DataType::Int32));
        assert!(matches!(result.data_types()[2], DataType::Int32));

        let page = result.page(0, 10);
        let batch = &page.batches[0];
        let binary_lengths = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let string_lengths = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(binary_lengths.value(0), 4);
        assert_eq!(binary_lengths.value(1), 0);
        assert!(binary_lengths.is_null(2));
        assert_eq!(string_lengths.value(0), 4);
        assert_eq!(string_lengths.value(1), 0);
        assert!(string_lengths.is_null(2));
    }

    #[tokio::test]
    async fn execute_allows_explain_select_queries() {
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            Arc::new(MockTableOpener::with_provider(mem_table_provider(
                int_batch(vec![1]),
            ))),
        );

        let result = engine
            .execute(
                "token",
                "EXPLAIN SELECT id FROM hits",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert!(result.row_count > 0);
    }

    #[tokio::test]
    async fn execute_show_schemas_forwards_token_and_skips_table_loading() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let opener = Arc::new(MockTableOpener::ok());
        let engine = QueryEngine::with_dependencies(test_config(), unity, opener.clone());

        let result = engine
            .execute("token-a", "SHOW SCHEMAS", "workspace", "default")
            .await
            .unwrap();

        assert_eq!(result.columns[0].name, "databaseName");
        assert_eq!(
            result_string_column(&result, 0),
            vec!["analytics".to_string(), "default".to_string()]
        );
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.schemas,
            vec![("token-a".to_string(), "workspace".to_string())]
        );
        assert!(calls.temporary_credentials.is_empty());
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn execute_show_catalogs_filters_and_forwards_token() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let engine =
            QueryEngine::with_dependencies(test_config(), unity, Arc::new(MockTableOpener::ok()));

        let result = engine
            .execute(
                "token-b",
                "SHOW CATALOGS LIKE 'work*'",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(result.columns[0].name, "catalog");
        assert_eq!(result_string_column(&result, 0), vec!["workspace"]);
        let calls = calls.lock().unwrap();
        assert_eq!(calls.catalogs, vec!["token-b".to_string()]);
        assert!(calls.temporary_credentials.is_empty());
    }

    #[tokio::test]
    async fn execute_show_tables_resolves_schema_filters_views_and_forwards_token() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let engine =
            QueryEngine::with_dependencies(test_config(), unity, Arc::new(MockTableOpener::ok()));

        let result = engine
            .execute(
                "token-c",
                "SHOW TABLES IN main.analytics LIKE 'fact*'",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["database", "tableName", "isTemporary"]
        );
        assert_eq!(result_string_column(&result, 0), vec!["analytics"]);
        assert_eq!(result_string_column(&result, 1), vec!["fact_sales"]);
        assert_eq!(result_bool_column(&result, 2), vec![false]);
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.tables,
            vec![(
                "token-c".to_string(),
                "main".to_string(),
                "analytics".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn execute_show_views_filters_tables_and_forwards_token() {
        let unity = Arc::new(RecordingUnity::new());
        let engine =
            QueryEngine::with_dependencies(test_config(), unity, Arc::new(MockTableOpener::ok()));

        let result = engine
            .execute(
                "token-d",
                "SHOW VIEWS IN analytics LIKE 'daily*'",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["namespace", "viewName", "isTemporary"]
        );
        assert_eq!(result_string_column(&result, 0), vec!["analytics"]);
        assert_eq!(result_string_column(&result, 1), vec!["daily_sales"]);
        assert_eq!(result_bool_column(&result, 2), vec![false]);
    }

    #[tokio::test]
    async fn execute_show_columns_resolves_table_and_forwards_token() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let opener = Arc::new(MockTableOpener::ok());
        let engine = QueryEngine::with_dependencies(test_config(), unity, opener.clone());

        let result = engine
            .execute(
                "token-columns",
                "SHOW COLUMNS IN fact_sales IN analytics",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(result.columns[0].name, "col_name");
        assert_eq!(
            result_string_column(&result, 0),
            vec!["cust_cd", "name", "cust_addr"]
        );
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.table,
            vec![(
                "token-columns".to_string(),
                "workspace.analytics.fact_sales".to_string()
            )]
        );
        assert!(calls.tables.is_empty());
        assert!(calls.temporary_credentials.is_empty());
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn get_columns_metadata_returns_databricks_column_rows() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let opener = Arc::new(MockTableOpener::ok());
        let engine = QueryEngine::with_dependencies(test_config(), unity, opener.clone());

        let result = engine
            .get_columns_metadata(
                "token-get-columns",
                GetColumnsMetadataRequest {
                    catalog: Some("workspace"),
                    schema: Some("analytics"),
                    table: Some("fact_sales"),
                    column: Some("cust_cd"),
                },
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "TYPE_NAME",
                "COLUMN_SIZE",
                "BUFFER_LENGTH",
                "DECIMAL_DIGITS",
                "NUM_PREC_RADIX",
                "NULLABLE",
                "REMARKS",
                "COLUMN_DEF",
                "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION",
                "IS_NULLABLE",
                "SCOPE_CATALOG",
                "SCOPE_SCHEMA",
                "SCOPE_TABLE",
                "SOURCE_DATA_TYPE",
                "IS_AUTO_INCREMENT",
            ]
        );
        assert_eq!(result_string_column(&result, 3), vec!["cust_cd"]);
        assert_eq!(result_i32_column(&result, 4), vec![-5]);
        assert_eq!(result_string_column(&result, 5), vec!["BIGINT"]);
        assert_eq!(result_i32_column(&result, 16), vec![1]);
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.table,
            vec![(
                "token-get-columns".to_string(),
                "workspace.analytics.fact_sales".to_string()
            )]
        );
        assert!(calls.temporary_credentials.is_empty());
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn get_columns_metadata_with_wildcard_table_pattern_returns_view_columns() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let opener = Arc::new(MockTableOpener::ok());
        let engine = QueryEngine::with_dependencies(test_config(), unity, opener.clone());

        let result = engine
            .get_columns_metadata(
                "token-get-columns",
                GetColumnsMetadataRequest {
                    catalog: Some("workspace"),
                    schema: Some("analytics"),
                    table: Some("daily%"),
                    column: Some("cust_cd"),
                },
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result_string_column(&result, 1), vec!["analytics"]);
        assert_eq!(result_string_column(&result, 2), vec!["daily_sales"]);
        assert_eq!(result_string_column(&result, 3), vec!["cust_cd"]);
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.tables,
            vec![(
                "token-get-columns".to_string(),
                "workspace".to_string(),
                "analytics".to_string()
            )]
        );
        assert_eq!(
            calls.table,
            vec![(
                "token-get-columns".to_string(),
                "workspace.analytics.daily_sales".to_string()
            )]
        );
        assert!(calls.temporary_credentials.is_empty());
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn get_columns_metadata_without_schema_lists_all_schemas() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let opener = Arc::new(MockTableOpener::ok());
        let engine = QueryEngine::with_dependencies(test_config(), unity, opener.clone());

        let result = engine
            .get_columns_metadata(
                "token-get-columns",
                GetColumnsMetadataRequest {
                    catalog: Some("workspace"),
                    schema: None,
                    table: Some("fact_sales"),
                    column: Some("cust_cd"),
                },
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 2);
        assert_eq!(
            result_string_column(&result, 1),
            vec!["analytics", "default"]
        );
        assert_eq!(
            result_string_column(&result, 2),
            vec!["fact_sales", "fact_sales"]
        );
        assert_eq!(result_string_column(&result, 3), vec!["cust_cd", "cust_cd"]);
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.schemas,
            vec![("token-get-columns".to_string(), "workspace".to_string())]
        );
        assert_eq!(
            calls.tables,
            vec![
                (
                    "token-get-columns".to_string(),
                    "workspace".to_string(),
                    "analytics".to_string()
                ),
                (
                    "token-get-columns".to_string(),
                    "workspace".to_string(),
                    "default".to_string()
                ),
            ]
        );
        assert_eq!(
            calls.table,
            vec![
                (
                    "token-get-columns".to_string(),
                    "workspace.analytics.fact_sales".to_string()
                ),
                (
                    "token-get-columns".to_string(),
                    "workspace.default.fact_sales".to_string()
                ),
            ]
        );
        assert!(calls.temporary_credentials.is_empty());
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn execute_show_table_extended_fetches_each_match_with_received_token() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let opener = Arc::new(MockTableOpener::ok());
        let engine = QueryEngine::with_dependencies(test_config(), unity, opener.clone());

        let result = engine
            .execute(
                "token-e",
                "SHOW TABLE EXTENDED IN analytics LIKE 'fact*'",
                "workspace",
                "default",
            )
            .await
            .unwrap();

        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["database", "tableName", "isTemporary", "information"]
        );
        assert_eq!(result_string_column(&result, 1), vec!["fact_sales"]);
        assert!(
            result_string_column(&result, 3)[0].contains("Provider: delta"),
            "{:?}",
            result_string_column(&result, 3)
        );
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.table,
            vec![(
                "token-e".to_string(),
                "workspace.analytics.fact_sales".to_string()
            )]
        );
        assert!(calls.temporary_credentials.is_empty());
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn execute_show_table_extended_partition_is_explicitly_unsupported() {
        let unity = Arc::new(RecordingUnity::new());
        let calls = unity.calls.clone();
        let engine =
            QueryEngine::with_dependencies(test_config(), unity, Arc::new(MockTableOpener::ok()));

        let err = engine
            .execute(
                "token-f",
                "SHOW TABLE EXTENDED IN analytics LIKE 'fact_sales' PARTITION (dt='2026-05-25')",
                "workspace",
                "default",
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, HarborError::UnsupportedSql(message) if message.contains("PARTITION"))
        );
        let calls = calls.lock().unwrap();
        assert!(calls.tables.is_empty());
        assert!(calls.table.is_empty());
    }

    #[tokio::test]
    async fn execute_rejects_ddl_without_opening_unity_tables() {
        let opener = Arc::new(MockTableOpener::ok());
        let engine = QueryEngine::with_dependencies(
            test_config(),
            Arc::new(MockUnity::delta()),
            opener.clone(),
        );

        let err = engine
            .execute(
                "token",
                "CREATE TABLE created_by_sql (id INT)",
                "workspace",
                "default",
            )
            .await
            .unwrap_err();

        assert!(matches!(err, HarborError::UnsupportedSql(_)));
        assert_eq!(opener.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn materialize_stream_preserves_schema_and_rows() {
        let batch = int_batch(vec![7, 8]);
        let stream: SendableRecordBatchStream =
            Box::pin(MemoryStream::try_new(vec![batch], test_schema(), None).unwrap());

        let result = materialize_stream(
            stream,
            ResultLimits {
                max_rows: Some(2),
                max_bytes: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.data_types(), &[DataType::Int32]);
    }

    enum MockTableResponse {
        Delta,
        NonDelta,
        Error(&'static str),
    }

    enum MockCredentialResponse {
        Ok,
        Error(&'static str),
    }

    #[derive(Default)]
    struct RecordingCalls {
        catalogs: Vec<String>,
        schemas: Vec<(String, String)>,
        tables: Vec<(String, String, String)>,
        table: Vec<(String, String)>,
        temporary_credentials: Vec<String>,
    }

    struct RecordingUnity {
        calls: Arc<Mutex<RecordingCalls>>,
    }

    impl RecordingUnity {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(RecordingCalls::default())),
            }
        }
    }

    #[async_trait::async_trait]
    impl UnityCatalog for RecordingUnity {
        async fn catalogs(&self, bearer_token: &str) -> Result<Vec<CatalogInfo>> {
            self.calls
                .lock()
                .unwrap()
                .catalogs
                .push(bearer_token.to_string());
            Ok(vec![
                CatalogInfo {
                    name: "main".to_string(),
                },
                CatalogInfo {
                    name: "workspace".to_string(),
                },
            ])
        }

        async fn schemas(&self, bearer_token: &str, catalog_name: &str) -> Result<Vec<SchemaInfo>> {
            self.calls
                .lock()
                .unwrap()
                .schemas
                .push((bearer_token.to_string(), catalog_name.to_string()));
            Ok(vec![
                SchemaInfo {
                    name: "default".to_string(),
                    full_name: Some(format!("{catalog_name}.default")),
                },
                SchemaInfo {
                    name: "analytics".to_string(),
                    full_name: Some(format!("{catalog_name}.analytics")),
                },
            ])
        }

        async fn tables(
            &self,
            bearer_token: &str,
            catalog_name: &str,
            schema_name: &str,
        ) -> Result<Vec<TableInfo>> {
            self.calls.lock().unwrap().tables.push((
                bearer_token.to_string(),
                catalog_name.to_string(),
                schema_name.to_string(),
            ));
            Ok(vec![
                metadata_table(
                    catalog_name,
                    schema_name,
                    "fact_sales",
                    "MANAGED",
                    Some("DELTA"),
                ),
                metadata_table(
                    catalog_name,
                    schema_name,
                    "dim_store",
                    "EXTERNAL",
                    Some("DELTA"),
                ),
                metadata_table(catalog_name, schema_name, "daily_sales", "VIEW", None),
            ])
        }

        async fn table(&self, bearer_token: &str, full_name: &str) -> Result<TableInfo> {
            self.calls
                .lock()
                .unwrap()
                .table
                .push((bearer_token.to_string(), full_name.to_string()));
            Ok(table_info(full_name, Some("DELTA"), true))
        }

        async fn temporary_table_credentials(
            &self,
            bearer_token: &str,
            _table_id: &str,
        ) -> Result<TemporaryTableCredentials> {
            self.calls
                .lock()
                .unwrap()
                .temporary_credentials
                .push(bearer_token.to_string());
            Ok(temporary_credentials())
        }
    }

    struct MockUnity {
        table_response: MockTableResponse,
        credential_response: MockCredentialResponse,
    }

    impl MockUnity {
        fn delta() -> Self {
            Self {
                table_response: MockTableResponse::Delta,
                credential_response: MockCredentialResponse::Ok,
            }
        }

        fn non_delta() -> Self {
            Self {
                table_response: MockTableResponse::NonDelta,
                credential_response: MockCredentialResponse::Ok,
            }
        }

        fn table_error(message: &'static str) -> Self {
            Self {
                table_response: MockTableResponse::Error(message),
                credential_response: MockCredentialResponse::Ok,
            }
        }

        fn credential_error(message: &'static str) -> Self {
            Self {
                table_response: MockTableResponse::Delta,
                credential_response: MockCredentialResponse::Error(message),
            }
        }
    }

    #[async_trait::async_trait]
    impl UnityCatalog for MockUnity {
        async fn catalogs(&self, _bearer_token: &str) -> Result<Vec<CatalogInfo>> {
            Ok(vec![
                CatalogInfo {
                    name: "workspace".to_string(),
                },
                CatalogInfo {
                    name: "main".to_string(),
                },
            ])
        }

        async fn schemas(
            &self,
            _bearer_token: &str,
            catalog_name: &str,
        ) -> Result<Vec<SchemaInfo>> {
            Ok(vec![
                SchemaInfo {
                    name: "default".to_string(),
                    full_name: Some(format!("{catalog_name}.default")),
                },
                SchemaInfo {
                    name: "analytics".to_string(),
                    full_name: Some(format!("{catalog_name}.analytics")),
                },
            ])
        }

        async fn tables(
            &self,
            _bearer_token: &str,
            catalog_name: &str,
            schema_name: &str,
        ) -> Result<Vec<TableInfo>> {
            Ok(vec![
                table_info(
                    &format!("{catalog_name}.{schema_name}.hits"),
                    Some("DELTA"),
                    true,
                ),
                TableInfo {
                    table_id: Some("view-id".to_string()),
                    full_name: format!("{catalog_name}.{schema_name}.daily_hits"),
                    name: Some("daily_hits".to_string()),
                    catalog_name: Some(catalog_name.to_string()),
                    schema_name: Some(schema_name.to_string()),
                    table_type: Some("VIEW".to_string()),
                    data_source_format: None,
                    storage_location: None,
                    comment: Some("daily hits view".to_string()),
                    created_by: Some("creator@example.com".to_string()),
                    columns: Vec::new(),
                },
            ])
        }

        async fn table(&self, _bearer_token: &str, full_name: &str) -> Result<TableInfo> {
            match self.table_response {
                MockTableResponse::Delta => Ok(table_info(full_name, Some("DELTA"), true)),
                MockTableResponse::NonDelta => Ok(table_info(full_name, Some("PARQUET"), true)),
                MockTableResponse::Error(message) => Err(HarborError::Unity(message.to_string())),
            }
        }

        async fn temporary_table_credentials(
            &self,
            _bearer_token: &str,
            _table_id: &str,
        ) -> Result<TemporaryTableCredentials> {
            match self.credential_response {
                MockCredentialResponse::Ok => Ok(temporary_credentials()),
                MockCredentialResponse::Error(message) => {
                    Err(HarborError::Unity(message.to_string()))
                }
            }
        }
    }

    enum MockOpenResponse {
        Ok(Arc<dyn TableProvider>),
        Error(&'static str),
    }

    struct MockTableOpener {
        response: MockOpenResponse,
        calls: AtomicUsize,
    }

    impl MockTableOpener {
        fn ok() -> Self {
            Self::with_provider(empty_table_provider())
        }

        fn with_provider(provider: Arc<dyn TableProvider>) -> Self {
            Self {
                response: MockOpenResponse::Ok(provider),
                calls: AtomicUsize::new(0),
            }
        }

        fn error(message: &'static str) -> Self {
            Self {
                response: MockOpenResponse::Error(message),
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl TableOpener for MockTableOpener {
        async fn open(
            &self,
            _credentials: &TemporaryTableCredentials,
            _aws_region: &str,
            credential_expires_at: Instant,
        ) -> Result<CachedTable> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match &self.response {
                MockOpenResponse::Ok(provider) => Ok(CachedTable::new(
                    provider.clone(),
                    Url::parse("s3://bench-bucket").unwrap(),
                    "ssb/sf10/tables/hits".to_string(),
                    Arc::new(InMemory::new()),
                    credential_expires_at,
                )),
                MockOpenResponse::Error(message) => Err(HarborError::Query(message.to_string())),
            }
        }
    }

    fn table_info(
        full_name: &str,
        data_source_format: Option<&str>,
        has_storage: bool,
    ) -> TableInfo {
        let mut parts = full_name.rsplitn(3, '.').collect::<Vec<_>>();
        parts.reverse();
        let (catalog_name, schema_name, name) = match parts.as_slice() {
            [catalog, schema, name] => (
                Some((*catalog).to_string()),
                Some((*schema).to_string()),
                Some((*name).to_string()),
            ),
            _ => (None, None, None),
        };
        TableInfo {
            table_id: Some("table-id".to_string()),
            full_name: full_name.to_string(),
            name,
            catalog_name,
            schema_name,
            table_type: Some("MANAGED".to_string()),
            data_source_format: data_source_format.map(str::to_string),
            storage_location: has_storage.then(|| "s3://bench-bucket/ssb/sf10/tables/hits".into()),
            comment: Some("test table".to_string()),
            created_by: Some("creator@example.com".to_string()),
            columns: test_columns(),
        }
    }

    fn metadata_table(
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_type: &str,
        data_source_format: Option<&str>,
    ) -> TableInfo {
        TableInfo {
            table_id: Some(format!("{table_name}-id")),
            full_name: format!("{catalog_name}.{schema_name}.{table_name}"),
            name: Some(table_name.to_string()),
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            table_type: Some(table_type.to_string()),
            data_source_format: data_source_format.map(str::to_string),
            storage_location: None,
            comment: None,
            created_by: Some("creator@example.com".to_string()),
            columns: Vec::new(),
        }
    }

    fn test_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "cust_cd".to_string(),
                position: Some(0),
                type_name: Some("BIGINT".to_string()),
                type_text: Some("bigint".to_string()),
                type_precision: None,
                type_scale: None,
                nullable: Some(true),
                comment: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                position: Some(1),
                type_name: Some("STRING".to_string()),
                type_text: Some("string".to_string()),
                type_precision: None,
                type_scale: None,
                nullable: Some(true),
                comment: None,
            },
            ColumnInfo {
                name: "cust_addr".to_string(),
                position: Some(2),
                type_name: Some("STRING".to_string()),
                type_text: Some("string".to_string()),
                type_precision: None,
                type_scale: None,
                nullable: Some(true),
                comment: None,
            },
        ]
    }

    fn temporary_credentials() -> TemporaryTableCredentials {
        TemporaryTableCredentials {
            aws_temp_credentials: AwsTempCredentials {
                access_key_id: "access-key".to_string(),
                secret_access_key: "secret-key".to_string(),
                session_token: "session-token".to_string(),
            },
            expiration_time: 4_102_444_800_000,
            url: "s3://bench-bucket/ssb/sf10/tables/hits".to_string(),
        }
    }

    fn test_config() -> Config {
        Config {
            bind_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            databricks_host: "https://workspace.cloud.databricks.com".to_string(),
            default_catalog: "workspace".to_string(),
            default_schema: "default".to_string(),
            aws_region: "us-west-2".to_string(),
            max_result_rows: Some(100),
            max_result_bytes: Some(usize::MAX),
            unity_request_timeout: Duration::from_secs(1),
            query_timeout: Duration::from_secs(30),
            idle_session_timeout: Duration::from_secs(60),
            completed_operation_ttl: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            max_sessions: 16,
            max_operations: 16,
            request_body_limit_bytes: 1024 * 1024,
            parquet_pushdown_filters: true,
            parquet_reorder_filters: true,
            target_partitions: 1,
            skip_partial_aggregation_probe_rows_threshold: 10_000,
            skip_partial_aggregation_probe_ratio_threshold: 0.8,
            table_cache_ttl: Duration::ZERO,
            table_cache_max_entries: 0,
            table_cache_credential_expiry_skew: Duration::ZERO,
            databricks_count_star_alias_rewrite: true,
            databricks_expression_alias_rewrite: true,
            unsafe_log_sql: false,
        }
    }

    fn empty_table_provider() -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(Schema::empty())))
    }

    fn mem_table_provider(batch: RecordBatch) -> Arc<dyn TableProvider> {
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap())
    }

    fn delta_type_nested_access_batch() -> RecordBatch {
        let c_struct_scalar = StructArray::from(vec![(
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("widget"), None])) as ArrayRef,
        )]);

        let child = StructArray::from(vec![(
            Arc::new(Field::new("effective_date", DataType::Date32, true)),
            Arc::new(Date32Array::from(vec![Some(19725), None])) as ArrayRef,
        )]);
        let c_struct_nested = StructArray::from(vec![(
            Arc::new(Field::new("child", child.data_type().clone(), true)),
            Arc::new(child) as ArrayRef,
        )]);

        let mut map_string_int = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map_string_int.keys().append_value("one");
        map_string_int.values().append_value(1);
        map_string_int.append(true).unwrap();
        map_string_int.append(true).unwrap();
        let c_map_string_int = map_string_int.finish();

        let mut map_string_array = MapBuilder::new(
            None,
            StringBuilder::new(),
            ListBuilder::new(Int32Builder::new()),
        );
        map_string_array.keys().append_value("small");
        map_string_array.values().values().append_value(1);
        map_string_array.values().values().append_value(2);
        map_string_array.values().append(true);
        map_string_array.append(true).unwrap();
        map_string_array.append(true).unwrap();
        let c_map_string_array = map_string_array.finish();

        let prices_map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Decimal128(10, 2), true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let price_map_builder = MapBuilder::new(
            None,
            StringBuilder::new(),
            Decimal128Builder::new().with_data_type(DataType::Decimal128(10, 2)),
        );
        let item_struct_builder = StructBuilder::new(
            vec![Field::new("prices", prices_map_type, true)],
            vec![Box::new(price_map_builder)],
        );
        let mut items_builder = ListBuilder::new(item_struct_builder);
        let item = items_builder.values();
        let prices = item
            .field_builder::<MapBuilder<StringBuilder, Decimal128Builder>>(0)
            .unwrap();
        prices.keys().append_value("usd");
        prices.values().append_value(1234);
        prices.append(true).unwrap();
        item.append(true);
        items_builder.append(true);
        items_builder.append(true);
        let items = items_builder.finish();
        let c_struct_all_complex = StructArray::from(vec![(
            Arc::new(Field::new("items", items.data_type().clone(), true)),
            Arc::new(items) as ArrayRef,
        )]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new("c_struct_scalar", c_struct_scalar.data_type().clone(), true),
            Field::new("c_struct_nested", c_struct_nested.data_type().clone(), true),
            Field::new(
                "c_map_string_int",
                c_map_string_int.data_type().clone(),
                true,
            ),
            Field::new(
                "c_map_string_array",
                c_map_string_array.data_type().clone(),
                true,
            ),
            Field::new(
                "c_struct_all_complex",
                c_struct_all_complex.data_type().clone(),
                true,
            ),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(c_struct_scalar) as ArrayRef,
                Arc::new(c_struct_nested) as ArrayRef,
                Arc::new(c_map_string_int) as ArrayRef,
                Arc::new(c_map_string_array) as ArrayRef,
                Arc::new(c_struct_all_complex) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn delta_type_scalar_length_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, false),
            Field::new("c_binary", DataType::Binary, true),
            Field::new("c_string", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![
                    Some(&b"\x00\x01\x02\xff"[..]),
                    Some(&b""[..]),
                    None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("josé"), Some(""), None])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn int_batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
        )
        .unwrap()
    }

    fn result_string_column(result: &QueryResult, column_index: usize) -> Vec<String> {
        let page = result.page(0, 100);
        let batch = &page.batches[0];
        let values = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..values.len())
            .map(|row| values.value(row).to_string())
            .collect()
    }

    fn result_bool_column(result: &QueryResult, column_index: usize) -> Vec<bool> {
        let page = result.page(0, 100);
        let batch = &page.batches[0];
        let values = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        (0..values.len()).map(|row| values.value(row)).collect()
    }

    fn result_i32_column(result: &QueryResult, column_index: usize) -> Vec<i32> {
        let page = result.page(0, 100);
        let batch = &page.batches[0];
        let values = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..values.len()).map(|row| values.value(row)).collect()
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn expect_cached_table_error(result: Result<CachedTable>) -> HarborError {
        match result {
            Ok(_) => panic!("expected cached table loading to fail"),
            Err(err) => err,
        }
    }
}
