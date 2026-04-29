use std::{error::Error, fmt, sync::Arc};

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
        DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
        FunctionArguments, GroupByExpr, Ident, ObjectName, ObjectNamePart, OrderBy, OrderByKind,
        Query, Select, SelectItem, SetExpr, Statement as SqlStatement, UnaryOperator, Value,
    },
    dialect::GenericDialect,
    parser::Parser,
};
use tokio::time::timeout;

use crate::{
    config::Config,
    error::{HarborError, Result},
    table_cache::TableCache,
    udf,
    unity::UnityCatalogClient,
};

mod catalog;
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
        match timeout(
            self.config.query_timeout,
            self.execute_inner(bearer_token, sql, default_catalog, default_schema),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(HarborError::Query(format!(
                "query exceeded HARBORSQL_QUERY_TIMEOUT_SECONDS={}",
                self.config.query_timeout.as_secs()
            ))),
        }
    }

    async fn execute_inner(
        &self,
        bearer_token: &str,
        sql: &str,
        default_catalog: &str,
        default_schema: &str,
    ) -> Result<QueryResult> {
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

        let execution_sql = rewrite_sql_fast_paths(sql);
        let dataframe = plan_sql(&ctx, &execution_sql).await?;
        for (object_store_url, routes) in object_store_routes.routes()? {
            ctx.register_object_store(
                &object_store_url,
                Arc::new(PrefixRoutingObjectStore::new(routes)),
            );
        }
        let stream = dataframe.execute_stream().await?;
        materialize_stream(
            stream,
            ResultLimits {
                max_rows: self.config.max_result_rows,
                max_bytes: self.config.max_result_bytes,
            },
        )
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

fn rewrite_sql_fast_paths(sql: &str) -> String {
    let dialect = GenericDialect {};
    let Ok(mut statements) = Parser::parse_sql(&dialect, sql) else {
        return sql.to_string();
    };
    if statements.len() != 1 {
        return sql.to_string();
    }
    let mut changed = false;

    if let Some(statement) = statements.first_mut() {
        changed = rewrite_statement_fast_paths(statement);
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

fn rewrite_statement_fast_paths(statement: &mut SqlStatement) -> bool {
    match statement {
        SqlStatement::Query(query) => rewrite_query_fast_paths(query),
        SqlStatement::Explain { statement, .. } => rewrite_statement_fast_paths(statement),
        _ => false,
    }
}

fn rewrite_query_fast_paths(query: &mut Query) -> bool {
    let mut changed = false;
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            changed |= rewrite_query_fast_paths(&mut cte.query);
        }
    }
    changed |= match query.body.as_mut() {
        SetExpr::Select(select) => rewrite_select_fast_paths(select),
        SetExpr::Query(query) => rewrite_query_fast_paths(query),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr_fast_paths(left) | rewrite_set_expr_fast_paths(right)
        }
        _ => false,
    };
    if let Some(order_by) = &mut query.order_by {
        changed |= rewrite_order_by_fast_paths(order_by);
    }
    changed
}

fn rewrite_set_expr_fast_paths(set_expr: &mut SetExpr) -> bool {
    match set_expr {
        SetExpr::Select(select) => rewrite_select_fast_paths(select),
        SetExpr::Query(query) => rewrite_query_fast_paths(query),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr_fast_paths(left) | rewrite_set_expr_fast_paths(right)
        }
        _ => false,
    }
}

fn rewrite_order_by_fast_paths(order_by: &mut OrderBy) -> bool {
    if let OrderByKind::Expressions(expressions) = &mut order_by.kind {
        expressions.iter_mut().fold(false, |changed, expression| {
            changed | rewrite_expr_fast_paths(&mut expression.expr)
        })
    } else {
        false
    }
}

fn rewrite_select_fast_paths(select: &mut Select) -> bool {
    let mut changed = false;
    for item in &mut select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => continue,
        };
        changed |= rewrite_expr_fast_paths(expr);
    }
    if let Some(selection) = &mut select.selection {
        changed |= rewrite_expr_fast_paths(selection);
    }
    if let Some(prewhere) = &mut select.prewhere {
        changed |= rewrite_expr_fast_paths(prewhere);
    }
    if let GroupByExpr::Expressions(expressions, _) = &mut select.group_by {
        for expression in &mut *expressions {
            changed |= rewrite_expr_fast_paths(expression);
        }
    }
    for expression in &mut select.cluster_by {
        changed |= rewrite_expr_fast_paths(expression);
    }
    for expression in &mut select.distribute_by {
        changed |= rewrite_expr_fast_paths(expression);
    }
    for order_by in &mut select.sort_by {
        changed |= rewrite_expr_fast_paths(&mut order_by.expr);
    }
    if let Some(having) = &mut select.having {
        changed |= rewrite_expr_fast_paths(having);
    }
    if let Some(qualify) = &mut select.qualify {
        changed |= rewrite_expr_fast_paths(qualify);
    }
    changed
}

fn rewrite_expr_fast_paths(expr: &mut Expr) -> bool {
    if rewrite_leaf_expr_fast_paths(expr) {
        return true;
    }

    match expr {
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            rewrite_expr_fast_paths(left) | rewrite_expr_fast_paths(right)
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
        | Expr::Prior(expr) => rewrite_expr_fast_paths(expr),
        _ => false,
    }
}

fn rewrite_leaf_expr_fast_paths(expr: &mut Expr) -> bool {
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
            if let Some((source, pattern, capture_index)) =
                regexp_replace_capture_fast_path_args(function)
            {
                *expr = regexp_replace_capture_expr(source, pattern, capture_index);
                true
            } else {
                rewrite_function_fast_paths(function)
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

fn rewrite_function_fast_paths(function: &mut Function) -> bool {
    let mut changed = false;
    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = &mut function.args {
        for arg in args {
            changed |= rewrite_function_arg_fast_paths(arg);
        }
    }
    changed
}

fn rewrite_function_arg_fast_paths(arg: &mut FunctionArg) -> bool {
    match arg {
        FunctionArg::Named { arg, .. }
        | FunctionArg::ExprNamed { arg, .. }
        | FunctionArg::Unnamed(arg) => match arg {
            FunctionArgExpr::Expr(expr) => rewrite_expr_fast_paths(expr),
            FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => false,
        },
    }
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
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant},
    };

    use datafusion::{
        arrow::{
            array::{ArrayRef, Int32Array},
            datatypes::{Field, Schema},
        },
        catalog::TableProvider,
        datasource::{MemTable, empty::EmptyTable},
        physical_plan::{SendableRecordBatchStream, memory::MemoryStream},
    };
    use object_store::memory::InMemory;
    use url::Url;

    use crate::{
        table_cache::CachedTable,
        unity::{AwsTempCredentials, TableInfo, TemporaryTableCredentials},
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

        assert!(rewritten.contains("contains(URL, 'google')"));
        assert!(rewritten.contains("NOT contains(URL, '.google.')"));
        assert!(!rewritten.contains(" LIKE "));
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
    fn leaves_complex_regexp_replace_unchanged() {
        let sql = "SELECT REGEXP_REPLACE(Referer, 'foo', 'bar') FROM hits";
        assert_eq!(rewrite_sql_fast_paths(sql), sql);
    }

    #[test]
    fn leaves_complex_like_predicates_unchanged() {
        let sql = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%goo_le%'";

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
        TableInfo {
            table_id: "table-id".to_string(),
            full_name: full_name.to_string(),
            data_source_format: data_source_format.map(str::to_string),
            storage_location: has_storage.then(|| "s3://bench-bucket/ssb/sf10/tables/hits".into()),
        }
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
        }
    }

    fn empty_table_provider() -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(Schema::empty())))
    }

    fn mem_table_provider(batch: RecordBatch) -> Arc<dyn TableProvider> {
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap())
    }

    fn int_batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
        )
        .unwrap()
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
