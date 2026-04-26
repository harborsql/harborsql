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
    ast::{ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableFactor},
    dialect::GenericDialect,
    parser::Parser,
};
use url::Url;

use crate::{
    config::Config,
    error::{HarborError, Result},
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
            unity: UnityCatalogClient::new(config.databricks_host.clone()),
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
        validate_select_only(sql)?;
        let refs = extract_table_refs(sql, default_catalog, default_schema)?;
        if refs.is_empty() {
            return Err(HarborError::UnsupportedSql(
                "no FROM/JOIN table references were found".into(),
            ));
        }

        let session_config = SessionConfig::new()
            .with_default_catalog_and_schema(default_catalog, default_schema)
            .set_bool("datafusion.sql_parser.enable_ident_normalization", false);
        let ctx = SessionContext::new_with_config(session_config);

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

        let dataframe = ctx.sql(sql).await?;
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
            if let Some(max_rows) = self.config.max_result_rows {
                if row_count > max_rows {
                    return Err(HarborError::Query(format!(
                        "query returned more than HARBORSQL_MAX_RESULT_ROWS={max_rows}",
                    )));
                }
            }

            writer.write(&batch)?;
            if let Some(max_bytes) = self.config.max_result_bytes {
                if writer.get_ref().len() > max_bytes {
                    return Err(HarborError::Query(format!(
                        "query result JSON exceeded HARBORSQL_MAX_RESULT_BYTES={max_bytes}",
                    )));
                }
            }
        }
        writer.finish()?;
        let buffer = writer.into_inner();
        if let Some(max_bytes) = self.config.max_result_bytes {
            if buffer.len() > max_bytes {
                return Err(HarborError::Query(format!(
                    "query result JSON is {} bytes, exceeding HARBORSQL_MAX_RESULT_BYTES={max_bytes}",
                    buffer.len(),
                )));
            }
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
        "table {} is not an externally readable Delta table (type={:?}, kind={:?}, format={:?}, storage={:?})",
        table.full_name,
        table.table_type,
        table.securable_kind,
        table.data_source_format,
        table.storage_location
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
            collect_query_table_refs(query, default_catalog, default_schema, &mut refs)?;
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
) -> Result<()> {
    let mut cte_names = BTreeSet::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_table_refs(&cte.query, default_catalog, default_schema, refs)?;
            cte_names.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }
    collect_set_expr_table_refs(
        &query.body,
        default_catalog,
        default_schema,
        refs,
        &cte_names,
    )
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
            collect_query_table_refs(query, default_catalog, default_schema, refs)
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
    for table_with_joins in &select.from {
        collect_table_factor_refs(
            &table_with_joins.relation,
            default_catalog,
            default_schema,
            refs,
            cte_names,
        )?;
        for join in &table_with_joins.joins {
            collect_table_factor_refs(
                &join.relation,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )?;
        }
    }
    Ok(())
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
            collect_query_table_refs(subquery, default_catalog, default_schema, refs)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_factor_refs(
                &table_with_joins.relation,
                default_catalog,
                default_schema,
                refs,
                cte_names,
            )?;
            for join in &table_with_joins.joins {
                collect_table_factor_refs(
                    &join.relation,
                    default_catalog,
                    default_schema,
                    refs,
                    cte_names,
                )?;
            }
            Ok(())
        }
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
}
