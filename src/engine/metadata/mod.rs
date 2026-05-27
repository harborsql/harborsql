use async_trait::async_trait;
use regex::{Regex, RegexBuilder};

use crate::{
    error::{HarborError, Result},
    unity::{CatalogInfo, ColumnInfo, SchemaInfo, TableInfo},
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
    Columns {
        table: ObjectName,
        schema: Option<ObjectName>,
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
struct ResolvedTable {
    catalog: String,
    schema: String,
    table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableRow {
    schema: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnMetadataRow {
    catalog: String,
    schema: String,
    table: String,
    column: String,
    data_type: i32,
    type_name: String,
    column_size: Option<i32>,
    decimal_digits: Option<i32>,
    num_prec_radix: Option<i32>,
    nullable: i32,
    remarks: Option<String>,
    sql_data_type: i32,
    sql_datetime_sub: Option<i32>,
    char_octet_length: Option<i32>,
    ordinal_position: i32,
    is_nullable: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableExtendedRow {
    schema: String,
    name: String,
    information: String,
}

#[derive(Debug, Clone)]
pub(super) struct GetColumnsRequest {
    pub(super) catalog: Option<String>,
    pub(super) schema: Option<String>,
    pub(super) table: Option<String>,
    pub(super) column: Option<String>,
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

pub(super) async fn get_columns(
    unity: &dyn UnityCatalog,
    bearer_token: &str,
    request: GetColumnsRequest,
    default_catalog: &str,
    _default_schema: &str,
) -> Result<QueryResult> {
    execute_get_columns(unity, bearer_token, request, default_catalog).await
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
        MetadataStatement::Columns { table, schema } => {
            let table = resolve_table(&table, schema.as_ref(), default_catalog, default_schema)?;
            let detailed = source
                .table(bearer_token, &table_full_identifier(&table))
                .await?;
            result::columns(table_column_names(&detailed))
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

async fn execute_get_columns<S>(
    source: &S,
    bearer_token: &str,
    request: GetColumnsRequest,
    default_catalog: &str,
) -> Result<QueryResult>
where
    S: MetadataSource + ?Sized,
{
    let request = normalize_get_columns_request(request);
    let catalogs = resolve_catalog_candidates(
        source,
        bearer_token,
        request.catalog.as_deref(),
        default_catalog,
    )
    .await?;
    let column_pattern = MetadataPattern::new(request.column.as_deref())?;
    let mut rows = Vec::new();

    for catalog in catalogs {
        let schemas =
            resolve_schema_candidates(source, bearer_token, &catalog, request.schema.as_deref())
                .await?;
        for schema in schemas {
            let tables = resolve_table_candidates(
                source,
                bearer_token,
                &catalog,
                &schema,
                request.table.as_deref(),
            )
            .await?;
            for table in tables {
                let full_name = table_full_identifier(&ResolvedTable {
                    catalog: catalog.clone(),
                    schema: schema.clone(),
                    table: table.name.clone(),
                });
                let detailed = source.table(bearer_token, &full_name).await?;
                rows.extend(column_metadata_rows(
                    &detailed,
                    &catalog,
                    &schema,
                    &table.name,
                    &column_pattern,
                ));
            }
        }
    }

    rows.sort_by(|left, right| {
        (
            left.catalog.to_ascii_lowercase(),
            left.schema.to_ascii_lowercase(),
            left.table.to_ascii_lowercase(),
            left.ordinal_position,
        )
            .cmp(&(
                right.catalog.to_ascii_lowercase(),
                right.schema.to_ascii_lowercase(),
                right.table.to_ascii_lowercase(),
                right.ordinal_position,
            ))
    });
    result::column_metadata(rows)
}

fn normalize_get_columns_request(mut request: GetColumnsRequest) -> GetColumnsRequest {
    if request.catalog.is_none()
        && request.schema.as_deref().is_some_and(contains_namespace)
        && let Some(schema) = request.schema.take()
    {
        let parts = schema.split('.').collect::<Vec<_>>();
        if parts.len() == 2 {
            request.catalog = Some(parts[0].to_string());
            request.schema = Some(parts[1].to_string());
        } else {
            request.schema = Some(schema);
        }
    }

    if request.table.as_deref().is_some_and(contains_namespace)
        && let Some(table) = request.table.take()
    {
        let parts = table.split('.').collect::<Vec<_>>();
        match parts.as_slice() {
            [catalog, schema, table] if request.catalog.is_none() && request.schema.is_none() => {
                request.catalog = Some((*catalog).to_string());
                request.schema = Some((*schema).to_string());
                request.table = Some((*table).to_string());
            }
            [schema, table] if request.schema.is_none() => {
                request.schema = Some((*schema).to_string());
                request.table = Some((*table).to_string());
            }
            _ => request.table = Some(table),
        }
    }

    request
}

async fn resolve_catalog_candidates<S>(
    source: &S,
    bearer_token: &str,
    catalog: Option<&str>,
    default_catalog: &str,
) -> Result<Vec<String>>
where
    S: MetadataSource + ?Sized,
{
    let Some(catalog) = catalog.filter(|value| !value.is_empty()) else {
        return Ok(vec![default_catalog.to_string()]);
    };
    if !contains_metadata_wildcard(catalog) {
        return Ok(vec![strip_metadata_escapes(catalog)]);
    }

    let pattern = MetadataPattern::new(Some(catalog))?;
    let mut catalogs = source
        .list_catalogs(bearer_token)
        .await?
        .into_iter()
        .map(|catalog| catalog.name)
        .filter(|name| pattern.matches(name))
        .collect::<Vec<_>>();
    catalogs.sort_by_key(|name| name.to_ascii_lowercase());
    Ok(catalogs)
}

async fn resolve_schema_candidates<S>(
    source: &S,
    bearer_token: &str,
    catalog: &str,
    schema: Option<&str>,
) -> Result<Vec<String>>
where
    S: MetadataSource + ?Sized,
{
    match schema {
        None => list_schema_candidates(source, bearer_token, catalog, None).await,
        Some("") => Ok(Vec::new()),
        Some(schema) if !contains_metadata_wildcard(schema) => {
            Ok(vec![strip_metadata_escapes(schema)])
        }
        Some(schema) => list_schema_candidates(source, bearer_token, catalog, Some(schema)).await,
    }
}

async fn list_schema_candidates<S>(
    source: &S,
    bearer_token: &str,
    catalog: &str,
    pattern: Option<&str>,
) -> Result<Vec<String>>
where
    S: MetadataSource + ?Sized,
{
    let pattern = MetadataPattern::new(pattern)?;
    let mut schemas = source
        .list_schemas(bearer_token, catalog)
        .await?
        .into_iter()
        .map(|schema| schema_name(&schema))
        .filter(|name| pattern.matches(name))
        .collect::<Vec<_>>();
    schemas.sort_by_key(|name| name.to_ascii_lowercase());
    Ok(schemas)
}

async fn resolve_table_candidates<S>(
    source: &S,
    bearer_token: &str,
    catalog: &str,
    schema: &str,
    table: Option<&str>,
) -> Result<Vec<TableRow>>
where
    S: MetadataSource + ?Sized,
{
    let Some(table) = table.filter(|value| !value.is_empty()) else {
        return list_matching_tables(source, bearer_token, catalog, schema, None).await;
    };
    if contains_metadata_wildcard(table) {
        return list_matching_tables(source, bearer_token, catalog, schema, Some(table)).await;
    }

    Ok(vec![TableRow {
        schema: schema.to_string(),
        name: strip_metadata_escapes(table),
    }])
}

async fn list_matching_tables<S>(
    source: &S,
    bearer_token: &str,
    catalog: &str,
    schema: &str,
    pattern: Option<&str>,
) -> Result<Vec<TableRow>>
where
    S: MetadataSource + ?Sized,
{
    let pattern = MetadataPattern::new(pattern)?;
    let mut rows = source
        .list_tables(bearer_token, catalog, schema)
        .await?
        .into_iter()
        .filter_map(|table| {
            let name = table_name(&table);
            pattern.matches(&name).then_some(TableRow {
                schema: schema.to_string(),
                name,
            })
        })
        .collect::<Vec<_>>();
    sort_table_rows(&mut rows);
    Ok(rows)
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

fn resolve_table(
    table_name: &ObjectName,
    schema_name: Option<&ObjectName>,
    default_catalog: &str,
    default_schema: &str,
) -> Result<ResolvedTable> {
    let (table_catalog, table_schema, table) = match table_name.0.as_slice() {
        [table] => (None, None, table.clone()),
        [schema, table] => (None, Some(schema.clone()), table.clone()),
        [catalog, schema, table] => (Some(catalog.clone()), Some(schema.clone()), table.clone()),
        _ => {
            return Err(HarborError::UnsupportedSql(
                "SHOW COLUMNS expects a table, schema.table, or catalog.schema.table name".into(),
            ));
        }
    };

    let (schema_catalog, schema) = match schema_name.map(|name| name.0.as_slice()) {
        None => (None, None),
        Some([schema]) => (None, Some(schema.clone())),
        Some([catalog, schema]) => (Some(catalog.clone()), Some(schema.clone())),
        Some(_) => {
            return Err(HarborError::UnsupportedSql(
                "SHOW COLUMNS expects an optional schema or catalog.schema name".into(),
            ));
        }
    };

    if let (Some(left), Some(right)) = (&table_schema, &schema)
        && left != right
    {
        return Err(HarborError::UnsupportedSql(
            "SHOW COLUMNS table name and schema name must not refer to different schemas".into(),
        ));
    }
    if let (Some(left), Some(right)) = (&table_catalog, &schema_catalog)
        && left != right
    {
        return Err(HarborError::UnsupportedSql(
            "SHOW COLUMNS table name and schema name must not refer to different catalogs".into(),
        ));
    }

    Ok(ResolvedTable {
        catalog: table_catalog
            .or(schema_catalog)
            .unwrap_or_else(|| default_catalog.to_string()),
        schema: table_schema
            .or(schema)
            .unwrap_or_else(|| default_schema.to_string()),
        table,
    })
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

fn table_full_identifier(table: &ResolvedTable) -> String {
    format!("{}.{}.{}", table.catalog, table.schema, table.table)
}

fn table_column_names(table: &TableInfo) -> Vec<String> {
    let mut columns = table.columns.iter().enumerate().collect::<Vec<_>>();
    columns.sort_by_key(|(index, column)| (column.position.unwrap_or(i32::MAX), *index));
    columns
        .into_iter()
        .map(|(_, column)| column.name.clone())
        .collect()
}

fn column_metadata_rows(
    table: &TableInfo,
    catalog: &str,
    schema: &str,
    table_name: &str,
    pattern: &MetadataPattern,
) -> Vec<ColumnMetadataRow> {
    let mut columns = table.columns.iter().enumerate().collect::<Vec<_>>();
    columns.sort_by_key(|(index, column)| (column.position.unwrap_or(i32::MAX), *index));
    columns
        .into_iter()
        .filter(|(_, column)| pattern.matches(&column.name))
        .map(|(index, column)| {
            let ordinal = column.position.unwrap_or(index as i32) + 1;
            let type_name = column_type_name(column);
            let data_type = jdbc_data_type(&type_name);
            let (nullable, is_nullable) = match column.nullable {
                Some(false) => (0, "NO"),
                Some(true) => (1, "YES"),
                None => (2, ""),
            };
            ColumnMetadataRow {
                catalog: table
                    .catalog_name
                    .clone()
                    .unwrap_or_else(|| catalog.to_string()),
                schema: table
                    .schema_name
                    .clone()
                    .unwrap_or_else(|| schema.to_string()),
                table: table.name.clone().unwrap_or_else(|| table_name.to_string()),
                column: column.name.clone(),
                data_type,
                type_name: type_name.clone(),
                column_size: column_size(&type_name, column),
                decimal_digits: decimal_digits(&type_name, column),
                num_prec_radix: num_prec_radix(&type_name),
                nullable,
                remarks: column.comment.clone().filter(|value| !value.is_empty()),
                sql_data_type: data_type,
                sql_datetime_sub: sql_datetime_sub(&type_name),
                char_octet_length: char_octet_length(&type_name),
                ordinal_position: ordinal,
                is_nullable: is_nullable.to_string(),
            }
        })
        .collect()
}

fn column_type_name(column: &ColumnInfo) -> String {
    column
        .type_text
        .as_deref()
        .or(column.type_name.as_deref())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| "STRING".to_string())
}

fn base_type_name(type_name: &str) -> &str {
    type_name
        .split(['(', '<'])
        .next()
        .unwrap_or(type_name)
        .trim()
}

fn jdbc_data_type(type_name: &str) -> i32 {
    match base_type_name(type_name) {
        "BOOLEAN" | "BOOL" => 16,
        "BYTE" | "TINYINT" => -6,
        "SHORT" | "SMALLINT" => 5,
        "INT" | "INTEGER" => 4,
        "LONG" | "BIGINT" => -5,
        "FLOAT" => 6,
        "DOUBLE" => 8,
        "DATE" => 91,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => 93,
        "BINARY" => -2,
        "DECIMAL" | "NUMERIC" => 3,
        "ARRAY" => 2003,
        "STRUCT" => 2002,
        "MAP" => 2000,
        _ => 12,
    }
}

fn column_size(type_name: &str, column: &ColumnInfo) -> Option<i32> {
    match base_type_name(type_name) {
        "DECIMAL" | "NUMERIC" => column.type_precision.or_else(|| parse_precision(type_name)),
        "BYTE" | "TINYINT" => Some(3),
        "SHORT" | "SMALLINT" => Some(5),
        "INT" | "INTEGER" | "DATE" => Some(10),
        "LONG" | "BIGINT" => Some(19),
        "FLOAT" => Some(7),
        "DOUBLE" => Some(15),
        "TIMESTAMP" | "TIMESTAMP_NTZ" => Some(29),
        "BOOLEAN" | "BOOL" | "BINARY" => Some(1),
        "STRING" | "VARCHAR" | "CHAR" => Some(255),
        "ARRAY" | "MAP" | "STRUCT" => Some(255),
        _ => None,
    }
}

fn decimal_digits(type_name: &str, column: &ColumnInfo) -> Option<i32> {
    match base_type_name(type_name) {
        "DECIMAL" | "NUMERIC" => column
            .type_scale
            .or_else(|| parse_scale(type_name))
            .or(Some(0)),
        "TIMESTAMP" | "TIMESTAMP_NTZ" => Some(9),
        _ => Some(0),
    }
}

fn num_prec_radix(type_name: &str) -> Option<i32> {
    match base_type_name(type_name) {
        "BYTE" | "TINYINT" | "SHORT" | "SMALLINT" | "INT" | "INTEGER" | "LONG" | "BIGINT"
        | "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => Some(10),
        _ => Some(0),
    }
}

fn sql_datetime_sub(type_name: &str) -> Option<i32> {
    match base_type_name(type_name) {
        "DATE" => Some(91),
        "TIMESTAMP" | "TIMESTAMP_NTZ" => Some(93),
        _ => None,
    }
}

fn char_octet_length(type_name: &str) -> Option<i32> {
    match base_type_name(type_name) {
        "STRING" | "VARCHAR" | "CHAR" => Some(255),
        "BINARY" => Some(32767),
        _ => None,
    }
}

fn parse_precision(type_name: &str) -> Option<i32> {
    parse_decimal_parts(type_name).and_then(|parts| parts.first().copied())
}

fn parse_scale(type_name: &str) -> Option<i32> {
    parse_decimal_parts(type_name).and_then(|parts| parts.get(1).copied())
}

fn parse_decimal_parts(type_name: &str) -> Option<Vec<i32>> {
    let start = type_name.find('(')?;
    let end = type_name[start + 1..].find(')')? + start + 1;
    type_name[start + 1..end]
        .split(',')
        .map(|part| part.trim().parse::<i32>().ok())
        .collect()
}

fn contains_namespace(value: &str) -> bool {
    value.contains('.')
}

fn contains_metadata_wildcard(value: &str) -> bool {
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if matches!(ch, '%' | '_' | '*') {
            return true;
        }
    }
    false
}

fn strip_metadata_escapes(value: &str) -> String {
    let mut stripped = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                stripped.push(next);
            } else {
                stripped.push(ch);
            }
        } else {
            stripped.push(ch);
        }
    }
    stripped
}

struct MetadataPattern {
    regex: Option<Regex>,
}

impl MetadataPattern {
    fn new(pattern: Option<&str>) -> Result<Self> {
        let Some(pattern) = pattern.filter(|value| !value.is_empty()) else {
            return Ok(Self { regex: None });
        };
        let regex = metadata_pattern_regex(pattern);
        let regex = RegexBuilder::new(&regex)
            .case_insensitive(true)
            .build()
            .map_err(|err| {
                HarborError::UnsupportedSql(format!("invalid metadata pattern `{pattern}`: {err}"))
            })?;
        Ok(Self { regex: Some(regex) })
    }

    fn matches(&self, value: &str) -> bool {
        self.regex
            .as_ref()
            .is_none_or(|regex| regex.is_match(value))
    }
}

fn metadata_pattern_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    let mut escaped = false;
    for ch in pattern.chars() {
        if escaped {
            regex.push_str(&regex::escape(&ch.to_string()));
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        match ch {
            '%' | '*' => regex.push_str(".*"),
            '_' => regex.push('.'),
            ch => regex.push_str(&regex::escape(&ch.to_string())),
        }
    }
    if escaped {
        regex.push_str(&regex::escape("\\"));
    }
    regex.push('$');
    regex
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
    use crate::unity::{ColumnInfo, TableInfo};

    use super::{
        GetColumnsRequest, MetadataPattern, ObjectName, contains_metadata_wildcard,
        normalize_get_columns_request, pattern::ShowPattern, resolve_schema, resolve_table,
    };

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
    fn resolves_show_columns_table_names() {
        assert_eq!(
            resolve_table(
                &ObjectName(vec!["customer".to_string()]),
                None,
                "workspace",
                "default",
            )
            .unwrap(),
            super::ResolvedTable {
                catalog: "workspace".to_string(),
                schema: "default".to_string(),
                table: "customer".to_string(),
            }
        );
        assert_eq!(
            resolve_table(
                &ObjectName(vec!["sales".to_string(), "customer".to_string()]),
                None,
                "workspace",
                "default",
            )
            .unwrap(),
            super::ResolvedTable {
                catalog: "workspace".to_string(),
                schema: "sales".to_string(),
                table: "customer".to_string(),
            }
        );
        assert_eq!(
            resolve_table(
                &ObjectName(vec!["customer".to_string()]),
                Some(&ObjectName(vec!["main".to_string(), "sales".to_string()])),
                "workspace",
                "default",
            )
            .unwrap(),
            super::ResolvedTable {
                catalog: "main".to_string(),
                schema: "sales".to_string(),
                table: "customer".to_string(),
            }
        );
    }

    #[test]
    fn rejects_conflicting_show_columns_namespaces() {
        assert!(
            resolve_table(
                &ObjectName(vec!["sales".to_string(), "customer".to_string()]),
                Some(&ObjectName(vec!["finance".to_string()])),
                "workspace",
                "default",
            )
            .is_err()
        );
    }

    #[test]
    fn get_columns_normalizes_dotted_names_with_underscores() {
        let request = normalize_get_columns_request(GetColumnsRequest {
            catalog: None,
            schema: None,
            table: Some("harborsql_clickbench_s3.hits_optimized".to_string()),
            column: None,
        });

        assert_eq!(request.catalog, None);
        assert_eq!(request.schema.as_deref(), Some("harborsql_clickbench_s3"));
        assert_eq!(request.table.as_deref(), Some("hits_optimized"));
    }

    #[test]
    fn metadata_patterns_honor_jdbc_escapes() {
        let escaped = MetadataPattern::new(Some(r"harborsql\_clickbench\_s3")).unwrap();
        assert!(escaped.matches("harborsql_clickbench_s3"));
        assert!(!escaped.matches("harborsqlXclickbenchYs3"));
        assert!(!contains_metadata_wildcard(r"hits\_optimized"));

        let wildcard = MetadataPattern::new(Some("hits_optim%")).unwrap();
        assert!(wildcard.matches("hits_optimized"));
        assert!(contains_metadata_wildcard("hits_optim%"));
    }

    #[test]
    fn column_metadata_maps_nullable_unknown_separately() {
        let table = TableInfo {
            table_id: Some("table-id".to_string()),
            full_name: "workspace.analytics.fact_sales".to_string(),
            name: Some("fact_sales".to_string()),
            catalog_name: Some("workspace".to_string()),
            schema_name: Some("analytics".to_string()),
            table_type: Some("MANAGED".to_string()),
            data_source_format: Some("DELTA".to_string()),
            storage_location: None,
            comment: None,
            created_by: None,
            columns: vec![
                ColumnInfo {
                    name: "required".to_string(),
                    position: Some(0),
                    type_name: Some("STRING".to_string()),
                    type_text: Some("string".to_string()),
                    type_precision: None,
                    type_scale: None,
                    nullable: Some(false),
                    comment: None,
                },
                ColumnInfo {
                    name: "optional".to_string(),
                    position: Some(1),
                    type_name: Some("STRING".to_string()),
                    type_text: Some("string".to_string()),
                    type_precision: None,
                    type_scale: None,
                    nullable: Some(true),
                    comment: None,
                },
                ColumnInfo {
                    name: "unknown".to_string(),
                    position: Some(2),
                    type_name: Some("STRING".to_string()),
                    type_text: Some("string".to_string()),
                    type_precision: None,
                    type_scale: None,
                    nullable: None,
                    comment: None,
                },
            ],
        };

        let rows = super::column_metadata_rows(
            &table,
            "workspace",
            "analytics",
            "fact_sales",
            &MetadataPattern::new(None).unwrap(),
        );

        assert_eq!(
            rows.iter()
                .map(|row| (row.column.as_str(), row.nullable, row.is_nullable.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("required", 0, "NO"),
                ("optional", 1, "YES"),
                ("unknown", 2, "")
            ]
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

    #[test]
    fn show_patterns_keep_regex_semantics_except_star_wildcards() {
        let pattern = ShowPattern::new(Some("fact.2026|dim_store")).unwrap();

        assert!(pattern.matches("fact_2026"));
        assert!(pattern.matches("fact.2026"));
        assert!(pattern.matches("dim_store"));
        assert!(!pattern.matches("dimXstore"));
    }

    #[test]
    fn show_patterns_reject_empty_or_invalid_patterns() {
        assert!(ShowPattern::new(Some("")).is_err());
        assert!(ShowPattern::new(Some("   ")).is_err());
        assert!(ShowPattern::new(Some("fact[")).is_err());
    }
}
