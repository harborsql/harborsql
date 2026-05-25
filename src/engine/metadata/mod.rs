use async_trait::async_trait;

use crate::{
    error::{HarborError, Result},
    unity::{CatalogInfo, SchemaInfo, TableInfo},
};

use super::{QueryResult, catalog::UnityCatalog};

mod parser;
mod pattern;
mod result;

#[derive(Debug, Clone, PartialEq, Eq)]
enum MetadataStatement {
    Catalogs {
        pattern: Option<String>,
    },
    Schemas {
        catalog: Option<ObjectName>,
        pattern: Option<String>,
    },
    Tables {
        schema: Option<ObjectName>,
        pattern: Option<String>,
    },
    Views {
        schema: Option<ObjectName>,
        pattern: Option<String>,
    },
    TableExtended {
        schema: Option<ObjectName>,
        pattern: String,
        partition: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectName(Vec<String>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedSchema {
    catalog: String,
    schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableRow {
    schema: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableExtendedRow {
    schema: String,
    name: String,
    information: String,
}

#[async_trait]
trait MetadataSource: Send + Sync {
    async fn list_catalogs(&self, bearer_token: &str) -> Result<Vec<CatalogInfo>>;

    async fn list_schemas(&self, bearer_token: &str, catalog_name: &str)
    -> Result<Vec<SchemaInfo>>;

    async fn list_tables(
        &self,
        bearer_token: &str,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<TableInfo>>;

    async fn table(&self, bearer_token: &str, full_name: &str) -> Result<TableInfo>;
}

#[async_trait]
impl<T> MetadataSource for T
where
    T: UnityCatalog + ?Sized,
{
    async fn list_catalogs(&self, bearer_token: &str) -> Result<Vec<CatalogInfo>> {
        self.catalogs(bearer_token).await
    }

    async fn list_schemas(
        &self,
        bearer_token: &str,
        catalog_name: &str,
    ) -> Result<Vec<SchemaInfo>> {
        self.schemas(bearer_token, catalog_name).await
    }

    async fn list_tables(
        &self,
        bearer_token: &str,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<TableInfo>> {
        self.tables(bearer_token, catalog_name, schema_name).await
    }

    async fn table(&self, bearer_token: &str, full_name: &str) -> Result<TableInfo> {
        UnityCatalog::table(self, bearer_token, full_name).await
    }
}

pub(super) async fn execute_show_statement(
    unity: &dyn UnityCatalog,
    bearer_token: &str,
    sql: &str,
    default_catalog: &str,
    default_schema: &str,
) -> Result<Option<QueryResult>> {
    let Some(statement) = parser::parse_show_statement(sql)? else {
        return Ok(None);
    };

    let result = execute_metadata_statement(
        unity,
        bearer_token,
        statement,
        default_catalog,
        default_schema,
    )
    .await?;
    Ok(Some(result))
}

async fn execute_metadata_statement<S>(
    source: &S,
    bearer_token: &str,
    statement: MetadataStatement,
    default_catalog: &str,
    default_schema: &str,
) -> Result<QueryResult>
where
    S: MetadataSource + ?Sized,
{
    match statement {
        MetadataStatement::Catalogs { pattern } => {
            let catalogs = source.list_catalogs(bearer_token).await?;
            let names = filtered_sorted_names(
                catalogs.into_iter().map(|catalog| catalog.name),
                pattern.as_deref(),
            )?;
            result::catalogs(names)
        }
        MetadataStatement::Schemas { catalog, pattern } => {
            let catalog = resolve_catalog(catalog.as_ref(), default_catalog)?;
            let schemas = source.list_schemas(bearer_token, &catalog).await?;
            let names = filtered_sorted_names(
                schemas.into_iter().map(|schema| schema_name(&schema)),
                pattern.as_deref(),
            )?;
            result::schemas(names)
        }
        MetadataStatement::Tables { schema, pattern } => {
            let schema = resolve_schema(schema.as_ref(), default_catalog, default_schema)?;
            let pattern = pattern::ShowPattern::new(pattern.as_deref())?;
            let mut rows = source
                .list_tables(bearer_token, &schema.catalog, &schema.schema)
                .await?
                .into_iter()
                .filter(is_table_like)
                .filter_map(|table| {
                    let name = table_name(&table);
                    pattern.matches(&name).then_some(TableRow {
                        schema: schema.schema.clone(),
                        name,
                    })
                })
                .collect::<Vec<_>>();
            sort_table_rows(&mut rows);
            result::tables(rows)
        }
        MetadataStatement::Views { schema, pattern } => {
            let schema = resolve_schema(schema.as_ref(), default_catalog, default_schema)?;
            let pattern = pattern::ShowPattern::new(pattern.as_deref())?;
            let mut rows = source
                .list_tables(bearer_token, &schema.catalog, &schema.schema)
                .await?
                .into_iter()
                .filter(is_view_like)
                .filter_map(|table| {
                    let name = table_name(&table);
                    pattern.matches(&name).then_some(TableRow {
                        schema: schema.schema.clone(),
                        name,
                    })
                })
                .collect::<Vec<_>>();
            sort_table_rows(&mut rows);
            result::views(rows)
        }
        MetadataStatement::TableExtended {
            schema,
            pattern,
            partition,
        } => {
            if partition.is_some() {
                return Err(HarborError::UnsupportedSql(
                    "SHOW TABLE EXTENDED PARTITION is not supported".into(),
                ));
            }

            let schema = resolve_schema(schema.as_ref(), default_catalog, default_schema)?;
            let pattern = pattern::ShowPattern::new(Some(&pattern))?;
            let tables = source
                .list_tables(bearer_token, &schema.catalog, &schema.schema)
                .await?;
            let mut rows = Vec::new();
            for table in tables.into_iter().filter(is_table_like) {
                let name = table_name(&table);
                if !pattern.matches(&name) {
                    continue;
                }
                let full_name = table_full_name(&table, &schema, &name);
                let detailed = source.table(bearer_token, &full_name).await?;
                rows.push(TableExtendedRow {
                    schema: schema.schema.clone(),
                    name,
                    information: table_information(&detailed),
                });
            }
            rows.sort_by(|left, right| {
                left.name
                    .to_ascii_lowercase()
                    .cmp(&right.name.to_ascii_lowercase())
            });
            result::table_extended(rows)
        }
    }
}

fn filtered_sorted_names<I>(names: I, pattern: Option<&str>) -> Result<Vec<String>>
where
    I: IntoIterator<Item = String>,
{
    let pattern = pattern::ShowPattern::new(pattern)?;
    let mut names = names
        .into_iter()
        .filter(|name| pattern.matches(name))
        .collect::<Vec<_>>();
    names.sort_by_key(|name| name.to_ascii_lowercase());
    Ok(names)
}

fn resolve_catalog(name: Option<&ObjectName>, default_catalog: &str) -> Result<String> {
    let Some(name) = name else {
        return Ok(default_catalog.to_string());
    };
    match name.0.as_slice() {
        [catalog] => Ok(catalog.clone()),
        _ => Err(HarborError::UnsupportedSql(
            "SHOW SCHEMAS expects a single catalog name".into(),
        )),
    }
}

fn resolve_schema(
    name: Option<&ObjectName>,
    default_catalog: &str,
    default_schema: &str,
) -> Result<ResolvedSchema> {
    match name.map(|name| name.0.as_slice()) {
        None => Ok(ResolvedSchema {
            catalog: default_catalog.to_string(),
            schema: default_schema.to_string(),
        }),
        Some([schema]) => Ok(ResolvedSchema {
            catalog: default_catalog.to_string(),
            schema: schema.clone(),
        }),
        Some([catalog, schema]) => Ok(ResolvedSchema {
            catalog: catalog.clone(),
            schema: schema.clone(),
        }),
        Some(_) => Err(HarborError::UnsupportedSql(
            "SHOW TABLES expects a schema or catalog.schema name".into(),
        )),
    }
}

fn schema_name(schema: &SchemaInfo) -> String {
    if !schema.name.is_empty() {
        return schema.name.clone();
    }
    schema
        .full_name
        .as_deref()
        .and_then(|full_name| full_name.rsplit('.').next())
        .unwrap_or("")
        .to_string()
}

fn table_name(table: &TableInfo) -> String {
    table
        .name
        .clone()
        .or_else(|| table.full_name.rsplit('.').next().map(str::to_string))
        .unwrap_or_default()
}

fn table_full_name(table: &TableInfo, schema: &ResolvedSchema, table_name: &str) -> String {
    if !table.full_name.is_empty() {
        table.full_name.clone()
    } else {
        format!("{}.{}.{}", schema.catalog, schema.schema, table_name)
    }
}

fn is_view_like(table: &TableInfo) -> bool {
    table
        .table_type
        .as_deref()
        .is_some_and(|table_type| matches!(table_type.to_ascii_uppercase().as_str(), "VIEW"))
}

fn is_table_like(table: &TableInfo) -> bool {
    !is_view_like(table)
}

fn sort_table_rows(rows: &mut [TableRow]) {
    rows.sort_by(|left, right| {
        left.name
            .to_ascii_lowercase()
            .cmp(&right.name.to_ascii_lowercase())
    });
}

fn table_information(table: &TableInfo) -> String {
    let mut lines = Vec::new();
    if let Some(catalog) = table
        .catalog_name
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("Catalog: {catalog}"));
    }
    if let Some(schema) = table
        .schema_name
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("Database: {schema}"));
    }
    lines.push(format!("Table: {}", table_name(table)));
    if let Some(table_type) = table
        .table_type
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("Type: {table_type}"));
    }
    if let Some(format) = table
        .data_source_format
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("Provider: {}", format.to_ascii_lowercase()));
    }
    if let Some(comment) = table.comment.as_deref().filter(|value| !value.is_empty()) {
        lines.push(format!("Comment: {comment}"));
    }
    if let Some(created_by) = table
        .created_by
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("Created By: {created_by}"));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::{ObjectName, pattern::ShowPattern, resolve_schema};

    #[test]
    fn resolves_show_table_namespaces() {
        assert_eq!(
            resolve_schema(None, "workspace", "default").unwrap(),
            super::ResolvedSchema {
                catalog: "workspace".to_string(),
                schema: "default".to_string(),
            }
        );
        assert_eq!(
            resolve_schema(
                Some(&ObjectName(vec!["analytics".to_string()])),
                "workspace",
                "default",
            )
            .unwrap(),
            super::ResolvedSchema {
                catalog: "workspace".to_string(),
                schema: "analytics".to_string(),
            }
        );
        assert_eq!(
            resolve_schema(
                Some(&ObjectName(vec![
                    "main".to_string(),
                    "analytics".to_string()
                ])),
                "workspace",
                "default",
            )
            .unwrap(),
            super::ResolvedSchema {
                catalog: "main".to_string(),
                schema: "analytics".to_string(),
            }
        );
    }

    #[test]
    fn show_patterns_match_databricks_wildcards_case_insensitively() {
        let pattern = ShowPattern::new(Some(" pay*|HR[a-z]+ ")).unwrap();

        assert!(pattern.matches("payments"));
        assert!(pattern.matches("PAYROLL"));
        assert!(pattern.matches("hrdata"));
        assert!(!pattern.matches("finance"));
    }
}
