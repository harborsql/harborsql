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
use regex::Regex;
use serde::Serialize;
use sqlparser::{dialect::GenericDialect, parser::Parser};
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

        let session_config =
            SessionConfig::new().with_default_catalog_and_schema(default_catalog, default_schema);
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
        let batches = dataframe.collect().await?;
        let row_count = batches.iter().map(|batch| batch.num_rows()).sum();
        let schema = batches
            .first()
            .map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| Column {
                        name: field.name().clone(),
                        data_type: field.data_type().to_string(),
                        nullable: field.is_nullable(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrayWriter::new(&mut buffer);
            for batch in &batches {
                writer.write(batch)?;
            }
            writer.finish()?;
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

    if matches!(
        statements.first(),
        Some(sqlparser::ast::Statement::Query(_))
    ) {
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
    let re = Regex::new(r#"(?i)\b(?:from|join)\s+([`"]?[A-Za-z_][A-Za-z0-9_\-]*[`"]?(?:\.[`"]?[A-Za-z_][A-Za-z0-9_\-]*[`"]?){0,2})"#)
        .map_err(|err| HarborError::Query(err.to_string()))?;
    let mut refs = BTreeSet::new();
    for capture in re.captures_iter(sql) {
        let raw = capture
            .get(1)
            .ok_or_else(|| HarborError::UnsupportedSql("malformed table reference".into()))?
            .as_str();
        refs.insert(resolve_table_ref(raw, default_catalog, default_schema)?);
    }

    Ok(refs.into_iter().collect())
}

fn resolve_table_ref(
    raw: &str,
    default_catalog: &str,
    default_schema: &str,
) -> Result<ResolvedTableRef> {
    let parts = raw
        .split('.')
        .map(strip_identifier_quotes)
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [table] => Ok(ResolvedTableRef::new(
            default_catalog,
            default_schema,
            table,
        )),
        [schema, table] => Ok(ResolvedTableRef::new(default_catalog, schema, table)),
        [catalog, schema, table] => Ok(ResolvedTableRef::new(catalog, schema, table)),
        _ => Err(HarborError::UnsupportedSql(format!(
            "unsupported table reference `{raw}`"
        ))),
    }
}

fn strip_identifier_quotes(value: &str) -> &str {
    value
        .strip_prefix('`')
        .and_then(|v| v.strip_suffix('`'))
        .or_else(|| value.strip_prefix('"').and_then(|v| v.strip_suffix('"')))
        .unwrap_or(value)
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

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: serde_json::Value,
    pub row_count: usize,
}

#[derive(Debug, Serialize)]
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
}
