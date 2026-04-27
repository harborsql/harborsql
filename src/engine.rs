use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use arrow_json::ArrayWriter;
use datafusion::{
    catalog::{
        CatalogProvider, MemoryCatalogProvider, SchemaProvider, memory::MemorySchemaProvider,
    },
    prelude::{SessionConfig, SessionContext},
};
use deltalake::open_table_with_storage_options;
use futures::StreamExt;
use serde::Serialize;
use sqlparser::{
    ast::{
        DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentClause,
        FunctionArgumentList, FunctionArguments, GroupByExpr, Ident, Join, JoinConstraint,
        JoinOperator, ObjectName, ObjectNamePart, OrderBy, OrderByKind, Query, Select, SelectItem,
        SetExpr, Statement, TableFactor, TableWithJoins, Value,
    },
    dialect::GenericDialect,
    parser::Parser,
};
use tokio::time::timeout;
use url::Url;

use crate::{
    config::Config,
    error::{HarborError, Result},
    udf,
    unity::{TemporaryTableCredentials, UnityCatalogClient},
};

#[derive(Clone)]
pub struct QueryEngine {
    config: Config,
    unity: UnityCatalogClient,
}

impl QueryEngine {
    pub fn new(config: Config) -> Self {
        Self {
            unity: UnityCatalogClient::new(
                config.databricks_host.clone(),
                config.unity_request_timeout,
            ),
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
        validate_select_only(sql)?;
        let refs = extract_table_refs(sql, default_catalog, default_schema)?;
        if refs.is_empty() {
            return Err(HarborError::UnsupportedSql(
                "no FROM/JOIN table references were found".into(),
            ));
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
            );
        let ctx = SessionContext::new_with_config(session_config);
        udf::register_udfs(&ctx);

        let mut catalogs: HashMap<String, Arc<MemoryCatalogProvider>> = HashMap::new();
        let mut schemas: HashMap<(String, String), Arc<MemorySchemaProvider>> = HashMap::new();

        for table_ref in refs {
            let full_name = table_ref.full_name();
            let table = self.unity.table(bearer_token, &full_name).await?;
            ensure_delta_table(&table)?;

            let credentials = self
                .unity
                .temporary_table_credentials(bearer_token, &table.table_id)
                .await?;
            let _credential_expiration_time_ms = credentials.expiration_time;
            let delta = open_table_with_storage_options(
                Url::parse(&credentials.url)?,
                storage_options(&credentials, &self.config.aws_region),
            )
            .await?;
            let provider = delta.table_provider().await?;

            let catalog = catalogs
                .entry(table_ref.catalog.clone())
                .or_insert_with(|| {
                    let catalog = Arc::new(MemoryCatalogProvider::new());
                    ctx.register_catalog(table_ref.catalog.clone(), catalog.clone());
                    catalog
                })
                .clone();

            let schema_key = (table_ref.catalog.clone(), table_ref.schema.clone());
            let schema = schemas
                .entry(schema_key)
                .or_insert_with(|| {
                    let schema = Arc::new(MemorySchemaProvider::new());
                    catalog
                        .register_schema(&table_ref.schema, schema.clone())
                        .expect("memory schema registration should not fail");
                    schema
                })
                .clone();

            schema
                .register_table(table_ref.table.clone(), provider)
                .map_err(HarborError::DataFusion)?;
        }

        let execution_sql = rewrite_clickbench_fast_paths(sql)?;
        let dataframe = ctx.sql(&execution_sql).await?;
        let mut stream = dataframe.execute_stream().await?;
        let schema = stream
            .schema()
            .fields()
            .iter()
            .map(|field| Column {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect();

        let mut row_count = 0;
        let mut writer = ArrayWriter::new(Vec::new());
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            row_count += batch.num_rows();
            if let Some(max_rows) = self.config.max_result_rows
                && row_count > max_rows
            {
                return Err(HarborError::Query(format!(
                    "query returned more than HARBORSQL_MAX_RESULT_ROWS={max_rows}",
                )));
            }

            writer.write(&batch)?;
            if let Some(max_bytes) = self.config.max_result_bytes
                && writer.get_ref().len() > max_bytes
            {
                return Err(HarborError::Query(format!(
                    "query result JSON exceeded HARBORSQL_MAX_RESULT_BYTES={max_bytes}",
                )));
            }
        }
        writer.finish()?;
        let buffer = writer.into_inner();
        if let Some(max_bytes) = self.config.max_result_bytes
            && buffer.len() > max_bytes
        {
            return Err(HarborError::Query(format!(
                "query result JSON is {} bytes, exceeding HARBORSQL_MAX_RESULT_BYTES={max_bytes}",
                buffer.len(),
            )));
        }
        let rows = serde_json::from_slice(&buffer)?;

        Ok(QueryResult {
            columns: schema,
            rows,
            row_count,
        })
    }
}

fn ensure_delta_table(table: &crate::unity::TableInfo) -> Result<()> {
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

fn validate_select_only(sql: &str) -> Result<()> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|err| HarborError::UnsupportedSql(err.to_string()))?;
    if statements.len() != 1 {
        return Err(HarborError::UnsupportedSql(
            "only one SQL statement is supported".into(),
        ));
    }

    if matches!(statements.first(), Some(Statement::Query(_))) {
        Ok(())
    } else {
        Err(HarborError::UnsupportedSql(
            "only read-only SELECT queries are supported".into(),
        ))
    }
}

fn rewrite_clickbench_fast_paths(sql: &str) -> Result<String> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|err| HarborError::UnsupportedSql(err.to_string()))?;
    let mut changed = false;

    if let Some(Statement::Query(query)) = statements.first_mut() {
        changed = rewrite_query_clickbench_fast_paths(query);
    }

    if changed {
        Ok(statements[0].to_string())
    } else {
        Ok(sql.to_string())
    }
}

fn rewrite_query_clickbench_fast_paths(query: &mut Query) -> bool {
    match query.body.as_mut() {
        SetExpr::Select(select) => rewrite_select_clickbench_fast_paths(select),
        SetExpr::Query(query) => rewrite_query_clickbench_fast_paths(query),
        _ => false,
    }
}

fn rewrite_select_clickbench_fast_paths(select: &mut Select) -> bool {
    let mut changed = false;
    for item in &mut select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => continue,
        };
        changed |= rewrite_expr_clickbench_fast_paths(expr);
    }
    changed
}

fn rewrite_expr_clickbench_fast_paths(expr: &mut Expr) -> bool {
    let (udf_name, source) = match expr {
        Expr::Function(function) => {
            let Some(source) = clickbench_referer_host_source(function) else {
                return false;
            };
            (udf::EXTRACT_REFERER_HOST_UDF, source)
        }
        Expr::Extract {
            field: DateTimeField::Minute,
            expr,
            ..
        } => (udf::EXTRACT_MINUTE_UDF, (**expr).clone()),
        _ => return false,
    };

    *expr = Expr::Function(Function {
        name: ObjectName::from(Ident::new(udf_name)),
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
    });
    true
}

fn clickbench_referer_host_source(function: &Function) -> Option<Expr> {
    if !function
        .name
        .to_string()
        .eq_ignore_ascii_case("regexp_replace")
    {
        return None;
    }
    if !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }

    let FunctionArguments::List(arguments) = &function.args else {
        return None;
    };
    if arguments.duplicate_treatment.is_some()
        || !arguments.clauses.is_empty()
        || arguments.args.len() != 3
    {
        return None;
    }

    let source = positional_expr_arg(&arguments.args[0])?.clone();
    let pattern = string_literal_value(positional_expr_arg(&arguments.args[1])?)?;
    let replacement = string_literal_value(positional_expr_arg(&arguments.args[2])?)?;

    if pattern == r"^https?://(?:www\.)?([^/]+)/.*$" && replacement == "$1" {
        Some(source)
    } else {
        None
    }
}

fn positional_expr_arg(arg: &FunctionArg) -> Option<&Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
        _ => None,
    }
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

fn extract_table_refs(
    sql: &str,
    default_catalog: &str,
    default_schema: &str,
) -> Result<Vec<ResolvedTableRef>> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|err| HarborError::UnsupportedSql(err.to_string()))?;
    if statements.len() != 1 {
        return Err(HarborError::UnsupportedSql(
            "only one SQL statement is supported".into(),
        ));
    }

    let mut refs = BTreeSet::new();
    match statements.first() {
        Some(Statement::Query(query)) => {
            collect_query_table_refs(
                query,
                default_catalog,
                default_schema,
                &mut refs,
                &BTreeSet::new(),
            )?;
        }
        _ => {
            return Err(HarborError::UnsupportedSql(
                "only read-only SELECT queries are supported".into(),
            ));
        }
    }

    Ok(refs.into_iter().collect())
}

fn collect_query_table_refs(
    query: &Query,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    outer_cte_names: &BTreeSet<String>,
) -> Result<()> {
    let mut cte_names = outer_cte_names.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_table_refs(
                &cte.query,
                default_catalog,
                default_schema,
                refs,
                &cte_names,
            )?;
            cte_names.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }
    collect_set_expr_table_refs(
        &query.body,
        default_catalog,
        default_schema,
        refs,
        &cte_names,
    )?;
    if let Some(order_by) = &query.order_by {
        collect_order_by_refs(order_by, default_catalog, default_schema, refs, &cte_names)?;
    }
    Ok(())
}

fn collect_set_expr_table_refs(
    set_expr: &SetExpr,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match set_expr {
        SetExpr::Select(select) => {
            collect_select_table_refs(select, default_catalog, default_schema, refs, cte_names)
        }
        SetExpr::Query(query) => {
            collect_query_table_refs(query, default_catalog, default_schema, refs, cte_names)
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_table_refs(left, default_catalog, default_schema, refs, cte_names)?;
            collect_set_expr_table_refs(right, default_catalog, default_schema, refs, cte_names)
        }
        SetExpr::Table(table) => {
            let table_name = table
                .table_name
                .as_deref()
                .ok_or_else(|| HarborError::UnsupportedSql("TABLE query needs a name".into()))?;
            let parts = if let Some(schema_name) = table.schema_name.as_deref() {
                vec![schema_name.to_string(), table_name.to_string()]
            } else {
                vec![table_name.to_string()]
            };
            if parts.len() == 1 && cte_names.contains(&parts[0].to_ascii_lowercase()) {
                return Ok(());
            }
            refs.insert(resolve_parts_ref(&parts, default_catalog, default_schema)?);
            Ok(())
        }
        SetExpr::Values(_) => Ok(()),
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => Err(
            HarborError::UnsupportedSql("only read-only SELECT queries are supported".into()),
        ),
    }
}

fn collect_select_table_refs(
    select: &Select,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    for item in &select.projection {
        collect_select_item_refs(item, default_catalog, default_schema, refs, cte_names)?;
    }
    for table_with_joins in &select.from {
        collect_table_with_joins_refs(
            table_with_joins,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        )?;
    }
    if let Some(prewhere) = &select.prewhere {
        collect_expr_table_refs(prewhere, default_catalog, default_schema, refs, cte_names)?;
    }
    if let Some(selection) = &select.selection {
        collect_expr_table_refs(selection, default_catalog, default_schema, refs, cte_names)?;
    }
    if let GroupByExpr::Expressions(expressions, _) = &select.group_by {
        for expression in expressions {
            collect_expr_table_refs(expression, default_catalog, default_schema, refs, cte_names)?;
        }
    }
    for expression in &select.cluster_by {
        collect_expr_table_refs(expression, default_catalog, default_schema, refs, cte_names)?;
    }
    for expression in &select.distribute_by {
        collect_expr_table_refs(expression, default_catalog, default_schema, refs, cte_names)?;
    }
    for order_by in &select.sort_by {
        collect_expr_table_refs(
            &order_by.expr,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        )?;
    }
    if let Some(having) = &select.having {
        collect_expr_table_refs(having, default_catalog, default_schema, refs, cte_names)?;
    }
    if let Some(qualify) = &select.qualify {
        collect_expr_table_refs(qualify, default_catalog, default_schema, refs, cte_names)?;
    }
    Ok(())
}

fn collect_select_item_refs(
    item: &SelectItem,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)
        }
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => Ok(()),
    }
}

fn collect_table_with_joins_refs(
    table_with_joins: &TableWithJoins,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    collect_table_factor_refs(
        &table_with_joins.relation,
        default_catalog,
        default_schema,
        refs,
        cte_names,
    )?;
    for join in &table_with_joins.joins {
        collect_join_refs(join, default_catalog, default_schema, refs, cte_names)?;
    }
    Ok(())
}

fn collect_join_refs(
    join: &Join,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    collect_table_factor_refs(
        &join.relation,
        default_catalog,
        default_schema,
        refs,
        cte_names,
    )?;
    collect_join_operator_refs(
        &join.join_operator,
        default_catalog,
        default_schema,
        refs,
        cte_names,
    )
}

fn collect_join_operator_refs(
    join_operator: &JoinOperator,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match join_operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => collect_join_constraint_refs(
            constraint,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        ),
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            collect_expr_table_refs(
                match_condition,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )?;
            collect_join_constraint_refs(
                constraint,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )
        }
        JoinOperator::CrossApply | JoinOperator::OuterApply => Ok(()),
    }
}

fn collect_join_constraint_refs(
    constraint: &JoinConstraint,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match constraint {
        JoinConstraint::On(expr) => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)
        }
        JoinConstraint::Using(_) | JoinConstraint::Natural | JoinConstraint::None => Ok(()),
    }
}

fn collect_order_by_refs(
    order_by: &OrderBy,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    if let OrderByKind::Expressions(expressions) = &order_by.kind {
        for expression in expressions {
            collect_expr_table_refs(
                &expression.expr,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )?;
        }
    }
    Ok(())
}

fn collect_expr_table_refs(
    expr: &Expr,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => collect_query_table_refs(query, default_catalog, default_schema, refs, cte_names),
        Expr::InSubquery { expr, subquery, .. } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            collect_query_table_refs(subquery, default_catalog, default_schema, refs, cte_names)
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            collect_exprs_table_refs(list, default_catalog, default_schema, refs, cte_names)
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(array_expr, default_catalog, default_schema, refs, cte_names)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(low, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(high, default_catalog, default_schema, refs, cte_names)
        }
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            collect_expr_table_refs(left, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(right, default_catalog, default_schema, refs, cte_names)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            collect_expr_table_refs(left, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(right, default_catalog, default_schema, refs, cte_names)
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(pattern, default_catalog, default_schema, refs, cte_names)
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
        | Expr::Prior(expr) => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)
        }
        Expr::IsNormalized { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Named { expr, .. } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)
        }
        Expr::Convert { expr, styles, .. } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            collect_exprs_table_refs(styles, default_catalog, default_schema, refs, cte_names)
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_expr_table_refs(timestamp, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(time_zone, default_catalog, default_schema, refs, cte_names)
        }
        Expr::Position { expr, r#in } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(r#in, default_catalog, default_schema, refs, cte_names)
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            if let Some(substring_from) = substring_from {
                collect_expr_table_refs(
                    substring_from,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            if let Some(substring_for) = substring_for {
                collect_expr_table_refs(
                    substring_for,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            Ok(())
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            if let Some(trim_what) = trim_what {
                collect_expr_table_refs(
                    trim_what,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            if let Some(trim_characters) = trim_characters {
                collect_exprs_table_refs(
                    trim_characters,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            Ok(())
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)?;
            collect_expr_table_refs(
                overlay_what,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )?;
            collect_expr_table_refs(
                overlay_from,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )?;
            if let Some(overlay_for) = overlay_for {
                collect_expr_table_refs(
                    overlay_for,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            Ok(())
        }
        Expr::CompoundFieldAccess { root, .. } => {
            collect_expr_table_refs(root, default_catalog, default_schema, refs, cte_names)
        }
        Expr::JsonAccess { value, .. } | Expr::Prefixed { value, .. } => {
            collect_expr_table_refs(value, default_catalog, default_schema, refs, cte_names)
        }
        Expr::Function(function) => {
            collect_function_table_refs(function, default_catalog, default_schema, refs, cte_names)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_table_refs(operand, default_catalog, default_schema, refs, cte_names)?;
            }
            for condition in conditions {
                collect_expr_table_refs(
                    &condition.condition,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
                collect_expr_table_refs(
                    &condition.result,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            if let Some(else_result) = else_result {
                collect_expr_table_refs(
                    else_result,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            Ok(())
        }
        Expr::Tuple(expressions) => collect_exprs_table_refs(
            expressions,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        ),
        Expr::GroupingSets(groups) | Expr::Cube(groups) | Expr::Rollup(groups) => {
            for group in groups {
                collect_exprs_table_refs(group, default_catalog, default_schema, refs, cte_names)?;
            }
            Ok(())
        }
        Expr::Struct { values, .. } => {
            collect_exprs_table_refs(values, default_catalog, default_schema, refs, cte_names)
        }
        Expr::Dictionary(fields) => {
            for field in fields {
                collect_expr_table_refs(
                    &field.value,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            Ok(())
        }
        Expr::Map(map) => {
            for entry in &map.entries {
                collect_expr_table_refs(
                    &entry.key,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
                collect_expr_table_refs(
                    &entry.value,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            Ok(())
        }
        Expr::Array(array) => collect_exprs_table_refs(
            &array.elem,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        ),
        Expr::Interval(interval) => collect_expr_table_refs(
            &interval.value,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        ),
        _ => Ok(()),
    }
}

fn collect_exprs_table_refs(
    expressions: &[Expr],
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    for expression in expressions {
        collect_expr_table_refs(expression, default_catalog, default_schema, refs, cte_names)?;
    }
    Ok(())
}

fn collect_function_table_refs(
    function: &Function,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    collect_function_arguments_table_refs(
        &function.parameters,
        default_catalog,
        default_schema,
        refs,
        cte_names,
    )?;
    collect_function_arguments_table_refs(
        &function.args,
        default_catalog,
        default_schema,
        refs,
        cte_names,
    )?;
    if let Some(filter) = &function.filter {
        collect_expr_table_refs(filter, default_catalog, default_schema, refs, cte_names)?;
    }
    for order_by in &function.within_group {
        collect_expr_table_refs(
            &order_by.expr,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        )?;
    }
    Ok(())
}

fn collect_function_arguments_table_refs(
    arguments: &FunctionArguments,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match arguments {
        FunctionArguments::None => Ok(()),
        FunctionArguments::Subquery(query) => {
            collect_query_table_refs(query, default_catalog, default_schema, refs, cte_names)
        }
        FunctionArguments::List(arguments) => {
            for argument in &arguments.args {
                collect_function_arg_table_refs(
                    argument,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            for clause in &arguments.clauses {
                match clause {
                    FunctionArgumentClause::OrderBy(order_by) => {
                        for expression in order_by {
                            collect_expr_table_refs(
                                &expression.expr,
                                default_catalog,
                                default_schema,
                                refs,
                                cte_names,
                            )?;
                        }
                    }
                    FunctionArgumentClause::Limit(expr) => {
                        collect_expr_table_refs(
                            expr,
                            default_catalog,
                            default_schema,
                            refs,
                            cte_names,
                        )?;
                    }
                    FunctionArgumentClause::Having(bound) => {
                        collect_expr_table_refs(
                            &bound.1,
                            default_catalog,
                            default_schema,
                            refs,
                            cte_names,
                        )?;
                    }
                    FunctionArgumentClause::IgnoreOrRespectNulls(_)
                    | FunctionArgumentClause::OnOverflow(_)
                    | FunctionArgumentClause::Separator(_)
                    | FunctionArgumentClause::JsonNullClause(_)
                    | FunctionArgumentClause::JsonReturningClause(_) => {}
                }
            }
            Ok(())
        }
    }
}

fn collect_function_arg_table_refs(
    argument: &FunctionArg,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match argument {
        FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
            collect_function_arg_expr_table_refs(
                arg,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )
        }
        FunctionArg::ExprNamed { name, arg, .. } => {
            collect_expr_table_refs(name, default_catalog, default_schema, refs, cte_names)?;
            collect_function_arg_expr_table_refs(
                arg,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )
        }
    }
}

fn collect_function_arg_expr_table_refs(
    argument: &FunctionArgExpr,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match argument {
        FunctionArgExpr::Expr(expr) => {
            collect_expr_table_refs(expr, default_catalog, default_schema, refs, cte_names)
        }
        FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => Ok(()),
    }
}

fn collect_table_factor_refs(
    table_factor: &TableFactor,
    default_catalog: &str,
    default_schema: &str,
    refs: &mut BTreeSet<ResolvedTableRef>,
    cte_names: &BTreeSet<String>,
) -> Result<()> {
    match table_factor {
        TableFactor::Table { name, args, .. } => {
            if args.is_some() {
                return Err(HarborError::UnsupportedSql(format!(
                    "table-valued functions are not supported: {name}"
                )));
            }
            let parts = object_name_parts(name)?;
            if parts.len() == 1 && cte_names.contains(&parts[0].to_ascii_lowercase()) {
                return Ok(());
            }
            refs.insert(resolve_parts_ref(&parts, default_catalog, default_schema)?);
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => {
            collect_query_table_refs(subquery, default_catalog, default_schema, refs, cte_names)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_table_with_joins_refs(
            table_with_joins,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        ),
        TableFactor::Pivot { table, .. } | TableFactor::Unpivot { table, .. } => {
            collect_table_factor_refs(table, default_catalog, default_schema, refs, cte_names)
        }
        other => Err(HarborError::UnsupportedSql(format!(
            "unsupported table factor `{other}`"
        ))),
    }
}

fn object_name_parts(name: &ObjectName) -> Result<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(identifier) => Ok(identifier.value.clone()),
            ObjectNamePart::Function(_) => Err(HarborError::UnsupportedSql(format!(
                "dynamic object names are not supported: {name}"
            ))),
        })
        .collect()
}

fn resolve_parts_ref(
    parts: &[String],
    default_catalog: &str,
    default_schema: &str,
) -> Result<ResolvedTableRef> {
    match parts {
        [table] => Ok(ResolvedTableRef::new(
            default_catalog,
            default_schema,
            table,
        )),
        [schema, table] => Ok(ResolvedTableRef::new(default_catalog, schema, table)),
        [catalog, schema, table] => Ok(ResolvedTableRef::new(catalog, schema, table)),
        _ => Err(HarborError::UnsupportedSql(format!(
            "unsupported table reference `{}`",
            parts.join(".")
        ))),
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ResolvedTableRef {
    catalog: String,
    schema: String,
    table: String,
}

impl ResolvedTableRef {
    fn new(catalog: &str, schema: &str, table: &str) -> Self {
        Self {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: serde_json::Value,
    pub row_count: usize,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: serde_json::Value::Array(Vec::new()),
            row_count: 0,
        }
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
    use super::*;

    #[test]
    fn resolves_table_names() {
        let refs = extract_table_refs(
            "SELECT * FROM workspace.analytics.events JOIN users ON events.user_id = users.id",
            "workspace",
            "default",
        )
        .unwrap();
        assert_eq!(
            refs,
            vec![
                ResolvedTableRef::new("workspace", "analytics", "events"),
                ResolvedTableRef::new("workspace", "default", "users"),
            ]
        );
    }

    #[test]
    fn ignores_from_inside_extract_expression() {
        let refs = extract_table_refs(
            "SELECT extract(minute FROM EventTime) AS m, COUNT(*) FROM hits GROUP BY m",
            "workspace",
            "default",
        )
        .unwrap();
        assert_eq!(
            refs,
            vec![ResolvedTableRef::new("workspace", "default", "hits")]
        );
    }

    #[test]
    fn finds_scalar_subquery_table_refs() {
        let refs = extract_table_refs(
            "SELECT id, (SELECT max(score) FROM scores) AS max_score \
             FROM users \
             WHERE id IN (SELECT user_id FROM purchases)",
            "workspace",
            "default",
        )
        .unwrap();
        assert_eq!(
            refs,
            vec![
                ResolvedTableRef::new("workspace", "default", "purchases"),
                ResolvedTableRef::new("workspace", "default", "scores"),
                ResolvedTableRef::new("workspace", "default", "users"),
            ]
        );
    }

    #[test]
    fn keeps_outer_ctes_visible_inside_derived_queries() {
        let refs = extract_table_refs(
            "WITH recent AS (SELECT * FROM analytics.events) \
             SELECT * FROM (SELECT * FROM recent) r \
             JOIN users u ON r.user_id = u.id",
            "workspace",
            "default",
        )
        .unwrap();
        assert_eq!(
            refs,
            vec![
                ResolvedTableRef::new("workspace", "analytics", "events"),
                ResolvedTableRef::new("workspace", "default", "users"),
            ]
        );
    }

    #[test]
    fn ctes_can_reference_previous_ctes() {
        let refs = extract_table_refs(
            "WITH base AS (SELECT * FROM analytics.events), \
             filtered AS (SELECT * FROM base) \
             SELECT * FROM filtered",
            "workspace",
            "default",
        )
        .unwrap();
        assert_eq!(
            refs,
            vec![ResolvedTableRef::new("workspace", "analytics", "events")]
        );
    }

    #[test]
    fn rewrites_clickbench_referer_regexp_replace() {
        let rewritten = rewrite_clickbench_fast_paths(
            "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '$1') AS k \
             FROM hits GROUP BY k",
        )
        .unwrap();

        assert!(rewritten.contains("harborsql_extract_referer_host(Referer) AS k"));
        assert!(!rewritten.contains("REGEXP_REPLACE"));
    }

    #[test]
    fn leaves_other_regexp_replace_calls_unchanged() {
        let sql = "SELECT REGEXP_REPLACE(Referer, 'x', '$1') AS k FROM hits";

        assert_eq!(rewrite_clickbench_fast_paths(sql).unwrap(), sql);
    }

    #[test]
    fn rewrites_clickbench_extract_minute() {
        let rewritten = rewrite_clickbench_fast_paths(
            "SELECT UserID, extract(minute FROM EventTime) AS m, COUNT(*) FROM hits GROUP BY UserID, m",
        )
        .unwrap();

        assert!(rewritten.contains("harborsql_extract_minute(EventTime) AS m"));
    }
}
