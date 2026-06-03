use std::{
    any::Any,
    collections::{BTreeSet, HashMap},
    fmt,
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, new_empty_array},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::ScalarValue,
    datasource::MemTable,
    error::DataFusionError,
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};

use crate::{
    error::{HarborError, Result},
    unity::{CatalogInfo, ColumnInfo, SchemaInfo, TableInfo},
};

use super::catalog::UnityCatalog;

const SYSTEM_CATALOG: &str = "system";
const INFORMATION_SCHEMA: &str = "information_schema";

#[derive(Clone)]
pub(super) struct SystemCatalogProvider {
    unity: Arc<dyn UnityCatalog>,
    bearer_token: Arc<str>,
}

impl SystemCatalogProvider {
    pub(super) fn new(unity: Arc<dyn UnityCatalog>, bearer_token: Arc<str>) -> Self {
        Self {
            unity,
            bearer_token,
        }
    }
}

impl fmt::Debug for SystemCatalogProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemCatalogProvider")
            .field("schemas", &self.schema_names())
            .finish_non_exhaustive()
    }
}

impl CatalogProvider for SystemCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![INFORMATION_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name.eq_ignore_ascii_case(INFORMATION_SCHEMA) {
            Some(information_schema(
                self.unity.clone(),
                self.bearer_token.clone(),
                None,
            ))
        } else {
            Some(Arc::new(SystemSchemaProvider::new(
                name,
                self.unity.clone(),
                self.bearer_token.clone(),
            )))
        }
    }
}

#[derive(Clone)]
struct SystemSchemaProvider {
    schema_name: String,
    unity: Arc<dyn UnityCatalog>,
    bearer_token: Arc<str>,
}

impl SystemSchemaProvider {
    fn new(schema_name: &str, unity: Arc<dyn UnityCatalog>, bearer_token: Arc<str>) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            unity,
            bearer_token,
        }
    }
}

impl fmt::Debug for SystemSchemaProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let full_name = format!("{SYSTEM_CATALOG}.{}.{}", self.schema_name, name);
        let table = self
            .unity
            .table(&self.bearer_token, &full_name)
            .await
            .map_err(to_datafusion_error)?;
        Ok(Some(Arc::new(SystemTableProvider::new(
            self.unity.clone(),
            self.bearer_token.clone(),
            table,
            SystemTableMode::DatabricksSystemTable,
            None,
        ))))
    }

    fn table_exist(&self, _name: &str) -> bool {
        false
    }
}

#[derive(Clone)]
pub(super) struct InformationSchemaProvider {
    unity: Arc<dyn UnityCatalog>,
    bearer_token: Arc<str>,
    catalog_scope: Option<String>,
}

impl InformationSchemaProvider {
    pub(super) fn new(
        unity: Arc<dyn UnityCatalog>,
        bearer_token: Arc<str>,
        catalog_scope: Option<String>,
    ) -> Self {
        Self {
            unity,
            bearer_token,
            catalog_scope,
        }
    }
}

impl fmt::Debug for InformationSchemaProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InformationSchemaProvider")
            .field("catalog_scope", &self.catalog_scope)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![
            "catalogs".to_string(),
            "columns".to_string(),
            "schemata".to_string(),
            "tables".to_string(),
            "views".to_string(),
        ]
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let full_name = format!("{SYSTEM_CATALOG}.{INFORMATION_SCHEMA}.{name}");
        let table = self
            .unity
            .table(&self.bearer_token, &full_name)
            .await
            .map_err(to_datafusion_error)?;
        Ok(Some(Arc::new(SystemTableProvider::new(
            self.unity.clone(),
            self.bearer_token.clone(),
            table,
            SystemTableMode::InformationSchema(information_schema_relation(name)),
            self.catalog_scope.clone(),
        ))))
    }

    async fn table_type(&self, _name: &str) -> datafusion::common::Result<Option<TableType>> {
        Ok(Some(TableType::View))
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug)]
enum SystemTableMode {
    InformationSchema(InformationSchemaRelation),
    DatabricksSystemTable,
}

#[derive(Clone, Copy, Debug)]
enum InformationSchemaRelation {
    Catalogs,
    Schemata,
    Tables,
    Columns,
    Views,
    Empty,
}

#[derive(Clone)]
struct SystemTableProvider {
    unity: Arc<dyn UnityCatalog>,
    bearer_token: Arc<str>,
    table: Arc<TableInfo>,
    mode: SystemTableMode,
    catalog_scope: Option<String>,
    schema: SchemaRef,
}

impl SystemTableProvider {
    fn new(
        unity: Arc<dyn UnityCatalog>,
        bearer_token: Arc<str>,
        table: TableInfo,
        mode: SystemTableMode,
        catalog_scope: Option<String>,
    ) -> Self {
        let schema = schema_from_columns(&table.columns);
        Self {
            unity,
            bearer_token,
            table: Arc::new(table),
            mode,
            catalog_scope,
            schema,
        }
    }

    async fn load_rows(&self, filters: &[Expr]) -> Result<Vec<SystemRow>> {
        match self.mode {
            SystemTableMode::InformationSchema(relation) => {
                let filters = MetadataFilters::from_exprs(filters);
                self.load_information_schema_rows(relation, &filters).await
            }
            SystemTableMode::DatabricksSystemTable => Err(HarborError::UnsupportedSql(format!(
                "Databricks system table {} uses {} storage and cannot be read until HarborSQL implements a system-table backend",
                self.table.full_name,
                self.table
                    .data_source_format
                    .as_deref()
                    .unwrap_or("Databricks-managed")
            ))),
        }
    }

    async fn load_information_schema_rows(
        &self,
        relation: InformationSchemaRelation,
        filters: &MetadataFilters,
    ) -> Result<Vec<SystemRow>> {
        match relation {
            InformationSchemaRelation::Catalogs => self.catalog_rows(filters).await,
            InformationSchemaRelation::Schemata => self.schemata_rows(filters).await,
            InformationSchemaRelation::Tables => self.table_rows(filters).await,
            InformationSchemaRelation::Columns => self.column_rows(filters).await,
            InformationSchemaRelation::Views => self.view_rows(filters).await,
            InformationSchemaRelation::Empty => Ok(Vec::new()),
        }
    }

    async fn catalog_rows(&self, filters: &MetadataFilters) -> Result<Vec<SystemRow>> {
        let rows = self
            .catalogs_for_catalog_rows(filters)
            .await?
            .into_iter()
            .map(|catalog| {
                let catalog_owner = if is_system_catalog(&catalog) {
                    "System user"
                } else {
                    ""
                };
                row([
                    ("catalog_name", string(catalog.name)),
                    ("catalog_owner", string(catalog_owner)),
                    ("comment", null_string()),
                    ("created", timestamp_millis(Some(0))),
                    ("created_by", string("")),
                    ("last_altered", timestamp_millis(Some(0))),
                    ("last_altered_by", string("")),
                ])
            })
            .collect();
        Ok(rows)
    }

    async fn catalogs_for_catalog_rows(
        &self,
        filters: &MetadataFilters,
    ) -> Result<Vec<CatalogInfo>> {
        let catalogs = self.unity.catalogs(&self.bearer_token).await?;
        if let Some(catalog_scope) = &self.catalog_scope {
            return Ok(catalogs
                .into_iter()
                .filter(|catalog| catalog.name.eq_ignore_ascii_case(catalog_scope))
                .filter(|catalog| filters.matches_catalog(&catalog.name))
                .collect());
        }

        Ok(catalogs
            .into_iter()
            .filter(|catalog| filters.matches_catalog(&catalog.name))
            .collect())
    }

    async fn schemata_rows(&self, filters: &MetadataFilters) -> Result<Vec<SystemRow>> {
        let mut rows = Vec::new();
        for catalog in self.catalogs(filters).await? {
            let catalog_name = catalog.name;
            for schema in self.schemas(&catalog_name, filters).await? {
                let schema_name = schema_name(&schema);
                rows.push(row([
                    ("catalog_name", string(catalog_name.clone())),
                    ("schema_name", string(schema_name)),
                    ("schema_owner", string("")),
                    ("comment", null_string()),
                    ("created", timestamp_millis(Some(0))),
                    ("created_by", string("")),
                    ("last_altered", timestamp_millis(Some(0))),
                    ("last_altered_by", string("")),
                ]));
            }
        }
        Ok(rows)
    }

    async fn table_rows(&self, filters: &MetadataFilters) -> Result<Vec<SystemRow>> {
        let mut rows = Vec::new();
        for catalog in self.catalogs(filters).await? {
            let catalog_name = catalog.name;
            for schema in self.schemas(&catalog_name, filters).await? {
                let schema_name = schema_name(&schema);
                for table in self
                    .unity
                    .tables(&self.bearer_token, &catalog_name, &schema_name)
                    .await?
                {
                    let table_name = table_name(&table);
                    if filters.matches_table(&table_name) {
                        rows.push(table_row(&catalog_name, &schema_name, &table_name, table));
                    }
                }
            }
        }
        sort_rows(&mut rows, ["table_catalog", "table_schema", "table_name"]);
        Ok(rows)
    }

    async fn column_rows(&self, filters: &MetadataFilters) -> Result<Vec<SystemRow>> {
        let mut rows = Vec::new();
        for catalog in self.catalogs(filters).await? {
            let catalog_name = catalog.name;
            for schema in self.schemas(&catalog_name, filters).await? {
                let schema_name = schema_name(&schema);
                for table in self
                    .unity
                    .tables(&self.bearer_token, &catalog_name, &schema_name)
                    .await?
                {
                    let table_name = table_name(&table);
                    if !filters.matches_table(&table_name) {
                        continue;
                    }
                    let full_name =
                        table_full_name(&table, &catalog_name, &schema_name, &table_name);
                    let detailed = self.unity.table(&self.bearer_token, &full_name).await?;
                    rows.extend(column_rows(
                        &detailed,
                        &catalog_name,
                        &schema_name,
                        &table_name,
                        filters,
                    ));
                }
            }
        }
        sort_rows(
            &mut rows,
            [
                "table_catalog",
                "table_schema",
                "table_name",
                "ordinal_position",
            ],
        );
        Ok(rows)
    }

    async fn view_rows(&self, filters: &MetadataFilters) -> Result<Vec<SystemRow>> {
        let mut rows = Vec::new();
        for catalog in self.catalogs(filters).await? {
            let catalog_name = catalog.name;
            for schema in self.schemas(&catalog_name, filters).await? {
                let schema_name = schema_name(&schema);
                for table in self
                    .unity
                    .tables(&self.bearer_token, &catalog_name, &schema_name)
                    .await?
                    .into_iter()
                    .filter(is_view_like)
                {
                    let table_name = table_name(&table);
                    if !filters.matches_table(&table_name) {
                        continue;
                    }
                    rows.push(row([
                        ("table_catalog", string(catalog_name.clone())),
                        ("table_schema", string(schema_name.clone())),
                        ("table_name", string(table_name)),
                        ("view_definition", null_string()),
                        ("check_option", string("NONE")),
                        ("is_updatable", string("NO")),
                        ("is_insertable_into", string("NO")),
                        ("sql_path", null_string()),
                        ("is_materialized", null_string()),
                    ]));
                }
            }
        }
        sort_rows(&mut rows, ["table_catalog", "table_schema", "table_name"]);
        Ok(rows)
    }

    async fn catalogs(&self, filters: &MetadataFilters) -> Result<Vec<CatalogInfo>> {
        if let Some(catalog) = &self.catalog_scope {
            if !filters.matches_catalog(catalog) {
                return Ok(Vec::new());
            }
            return Ok(vec![CatalogInfo {
                name: catalog.clone(),
                catalog_type: None,
            }]);
        }

        Ok(self
            .unity
            .catalogs(&self.bearer_token)
            .await?
            .into_iter()
            .filter(|catalog| filters.matches_catalog(&catalog.name))
            .collect())
    }

    async fn schemas(&self, catalog: &str, filters: &MetadataFilters) -> Result<Vec<SchemaInfo>> {
        Ok(self
            .unity
            .schemas(&self.bearer_token, catalog)
            .await?
            .into_iter()
            .filter(|schema| filters.matches_schema(&schema_name(schema)))
            .collect())
    }
}

impl fmt::Debug for SystemTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemTableProvider")
            .field("table", &self.table.full_name)
            .field("mode", &self.mode)
            .field("catalog_scope", &self.catalog_scope)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for SystemTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let rows = if limit == Some(0) {
            Vec::new()
        } else {
            self.load_rows(filters).await.map_err(to_datafusion_error)?
        };
        let batch = record_batch_from_rows(self.schema(), rows).map_err(to_datafusion_error)?;
        let table = MemTable::try_new(self.schema(), vec![vec![batch]])?;
        table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

#[derive(Debug, Default)]
struct MetadataFilters {
    catalogs: Option<BTreeSet<String>>,
    schemas: Option<BTreeSet<String>>,
    tables: Option<BTreeSet<String>>,
    columns: Option<BTreeSet<String>>,
}

impl MetadataFilters {
    fn from_exprs(filters: &[Expr]) -> Self {
        let mut metadata_filters = Self::default();
        for filter in filters {
            metadata_filters.collect(filter);
        }
        metadata_filters
    }

    fn collect(&mut self, expr: &Expr) {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                self.collect(&binary.left);
                self.collect(&binary.right);
            }
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                if let Some((column, value)) = equality_filter(&binary.left, &binary.right) {
                    self.add(column, value);
                } else if let Some((column, value)) = equality_filter(&binary.right, &binary.left) {
                    self.add(column, value);
                }
            }
            _ => {}
        }
    }

    fn add(&mut self, column: &str, value: &str) {
        match column.to_ascii_lowercase().as_str() {
            "catalog_name" | "table_catalog" => insert_filter_value(&mut self.catalogs, value),
            "schema_name" | "table_schema" => insert_filter_value(&mut self.schemas, value),
            "table_name" => insert_filter_value(&mut self.tables, value),
            "column_name" => insert_filter_value(&mut self.columns, value),
            _ => {}
        }
    }

    fn matches_catalog(&self, catalog: &str) -> bool {
        self.catalogs
            .as_ref()
            .is_none_or(|catalogs| catalogs.contains(&catalog.to_ascii_lowercase()))
    }

    fn matches_schema(&self, schema: &str) -> bool {
        self.schemas
            .as_ref()
            .is_none_or(|schemas| schemas.contains(&schema.to_ascii_lowercase()))
    }

    fn matches_table(&self, table: &str) -> bool {
        self.tables
            .as_ref()
            .is_none_or(|tables| tables.contains(&table.to_ascii_lowercase()))
    }

    fn matches_column(&self, column: &str) -> bool {
        self.columns
            .as_ref()
            .is_none_or(|columns| columns.contains(&column.to_ascii_lowercase()))
    }
}

type SystemRow = HashMap<&'static str, ScalarValue>;

pub(super) fn information_schema(
    unity: Arc<dyn UnityCatalog>,
    bearer_token: Arc<str>,
    catalog_scope: Option<String>,
) -> Arc<dyn SchemaProvider> {
    Arc::new(InformationSchemaProvider::new(
        unity,
        bearer_token,
        catalog_scope,
    ))
}

fn information_schema_relation(name: &str) -> InformationSchemaRelation {
    match name.to_ascii_lowercase().as_str() {
        "catalogs" => InformationSchemaRelation::Catalogs,
        "schemata" => InformationSchemaRelation::Schemata,
        "tables" => InformationSchemaRelation::Tables,
        "columns" => InformationSchemaRelation::Columns,
        "views" => InformationSchemaRelation::Views,
        _ => InformationSchemaRelation::Empty,
    }
}

fn schema_from_columns(columns: &[ColumnInfo]) -> SchemaRef {
    let mut columns = columns.iter().enumerate().collect::<Vec<_>>();
    columns.sort_by_key(|(index, column)| (column.position.unwrap_or(i32::MAX), *index));
    Arc::new(Schema::new(
        columns
            .into_iter()
            .map(|(_, column)| {
                Field::new(
                    column.name.clone(),
                    arrow_type(column),
                    column.nullable.unwrap_or(true),
                )
            })
            .collect::<Vec<_>>(),
    ))
}

fn arrow_type(column: &ColumnInfo) -> DataType {
    let type_name = column
        .type_text
        .as_deref()
        .or(column.type_name.as_deref())
        .unwrap_or("string");
    match base_type_name(type_name).to_ascii_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "BYTE" | "TINYINT" => DataType::Int8,
        "SHORT" | "SMALLINT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "LONG" | "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Millisecond, None),
        _ => DataType::Utf8,
    }
}

fn record_batch_from_rows(schema: SchemaRef, rows: Vec<SystemRow>) -> Result<RecordBatch> {
    let arrays = if rows.is_empty() {
        schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect::<Vec<_>>()
    } else {
        schema
            .fields()
            .iter()
            .map(|field| {
                let values = rows
                    .iter()
                    .map(|row| {
                        row.get(field.name().as_str())
                            .cloned()
                            .unwrap_or_else(|| default_scalar(field))
                    })
                    .collect::<Vec<_>>();
                ScalarValue::iter_to_array(values).map_err(HarborError::from)
            })
            .collect::<Result<Vec<ArrayRef>>>()?
    };
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn table_row(
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    table: TableInfo,
) -> SystemRow {
    let table_type = table.table_type.clone().unwrap_or_else(|| "MANAGED".into());
    let created_by = table.created_by.clone().unwrap_or_default();
    let is_insertable_into = if is_view_like(&table) { "NO" } else { "YES" };
    row([
        (
            "table_catalog",
            string(
                table
                    .catalog_name
                    .unwrap_or_else(|| catalog_name.to_string()),
            ),
        ),
        (
            "table_schema",
            string(table.schema_name.unwrap_or_else(|| schema_name.to_string())),
        ),
        (
            "table_name",
            string(table.name.unwrap_or_else(|| table_name.to_string())),
        ),
        ("table_type", string(table_type)),
        ("is_insertable_into", string(is_insertable_into)),
        ("commit_action", string("PRESERVE")),
        (
            "table_owner",
            string(
                table
                    .owner
                    .or_else(|| table.created_by.clone())
                    .unwrap_or_default(),
            ),
        ),
        ("comment", nullable_string(table.comment)),
        ("created", timestamp_millis(table.created_at.or(Some(0)))),
        ("created_by", string(created_by.clone())),
        (
            "last_altered",
            timestamp_millis(table.updated_at.or(table.created_at).or(Some(0))),
        ),
        (
            "last_altered_by",
            string(table.updated_by.or(Some(created_by)).unwrap_or_default()),
        ),
        (
            "data_source_format",
            string(table.data_source_format.unwrap_or_default()),
        ),
        ("storage_sub_directory", null_string()),
        ("storage_path", nullable_string(table.storage_location)),
    ])
}

fn column_rows(
    table: &TableInfo,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    filters: &MetadataFilters,
) -> Vec<SystemRow> {
    let mut columns = table.columns.iter().enumerate().collect::<Vec<_>>();
    columns.sort_by_key(|(index, column)| (column.position.unwrap_or(i32::MAX), *index));
    columns
        .into_iter()
        .filter(|(_, column)| filters.matches_column(&column.name))
        .map(|(index, column)| {
            let type_name = column_type_name(column);
            row([
                (
                    "table_catalog",
                    string(
                        table
                            .catalog_name
                            .clone()
                            .unwrap_or_else(|| catalog_name.to_string()),
                    ),
                ),
                (
                    "table_schema",
                    string(
                        table
                            .schema_name
                            .clone()
                            .unwrap_or_else(|| schema_name.to_string()),
                    ),
                ),
                (
                    "table_name",
                    string(table.name.clone().unwrap_or_else(|| table_name.to_string())),
                ),
                ("column_name", string(column.name.clone())),
                (
                    "ordinal_position",
                    ScalarValue::Int32(Some(column.position.unwrap_or(index as i32) + 1)),
                ),
                ("column_default", null_string()),
                (
                    "is_nullable",
                    string(if column.nullable.unwrap_or(true) {
                        "YES"
                    } else {
                        "NO"
                    }),
                ),
                ("full_data_type", string(type_name.clone())),
                (
                    "data_type",
                    string(base_type_name(&type_name).to_ascii_uppercase()),
                ),
                ("character_maximum_length", ScalarValue::Int64(None)),
                ("character_octet_length", ScalarValue::Int64(None)),
                ("numeric_precision", ScalarValue::Int32(None)),
                ("numeric_precision_radix", ScalarValue::Int32(None)),
                ("numeric_scale", ScalarValue::Int32(None)),
                ("datetime_precision", ScalarValue::Int32(None)),
                ("interval_type", null_string()),
                ("interval_precision", ScalarValue::Int32(None)),
                ("maximum_cardinality", ScalarValue::Int64(None)),
                ("is_identity", string("NO")),
                ("identity_generation", null_string()),
                ("identity_start", null_string()),
                ("identity_increment", null_string()),
                ("identity_maximum", null_string()),
                ("identity_minimum", null_string()),
                ("identity_cycle", null_string()),
                ("is_generated", string("NEVER")),
                ("generation_expression", null_string()),
                ("is_system_time_period_start", string("NO")),
                ("is_system_time_period_end", string("NO")),
                ("system_time_period_timestamp_generation", null_string()),
                ("is_updatable", string("YES")),
                ("partition_index", ScalarValue::Int32(None)),
                ("comment", nullable_string(column.comment.clone())),
            ])
        })
        .collect()
}

fn equality_filter<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a str, &'a str)> {
    let Expr::Column(column) = left else {
        return None;
    };
    Some((column.name.as_str(), string_literal(right)?))
}

fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) => Some(value),
        _ => None,
    }
}

fn row<const N: usize>(values: [(&'static str, ScalarValue); N]) -> SystemRow {
    values.into_iter().collect()
}

fn string(value: impl Into<String>) -> ScalarValue {
    ScalarValue::Utf8(Some(value.into()))
}

fn nullable_string(value: Option<String>) -> ScalarValue {
    ScalarValue::Utf8(value)
}

fn null_string() -> ScalarValue {
    ScalarValue::Utf8(None)
}

fn timestamp_millis(value: Option<i64>) -> ScalarValue {
    ScalarValue::TimestampMillisecond(value, None)
}

fn default_scalar(field: &Field) -> ScalarValue {
    if field.is_nullable() {
        return null_scalar(field.data_type());
    }
    match field.data_type() {
        DataType::Boolean => ScalarValue::Boolean(Some(false)),
        DataType::Int8 => ScalarValue::Int8(Some(0)),
        DataType::Int16 => ScalarValue::Int16(Some(0)),
        DataType::Int32 => ScalarValue::Int32(Some(0)),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        DataType::Float32 => ScalarValue::Float32(Some(0.0)),
        DataType::Float64 => ScalarValue::Float64(Some(0.0)),
        DataType::Date32 => ScalarValue::Date32(Some(0)),
        DataType::Timestamp(TimeUnit::Millisecond, None) => timestamp_millis(Some(0)),
        _ => string(""),
    }
}

fn null_scalar(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Boolean => ScalarValue::Boolean(None),
        DataType::Int8 => ScalarValue::Int8(None),
        DataType::Int16 => ScalarValue::Int16(None),
        DataType::Int32 => ScalarValue::Int32(None),
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::Float32 => ScalarValue::Float32(None),
        DataType::Float64 => ScalarValue::Float64(None),
        DataType::Date32 => ScalarValue::Date32(None),
        DataType::Timestamp(TimeUnit::Millisecond, None) => timestamp_millis(None),
        _ => null_string(),
    }
}

fn insert_filter_value(filters: &mut Option<BTreeSet<String>>, value: &str) {
    filters
        .get_or_insert_with(BTreeSet::new)
        .insert(value.to_ascii_lowercase());
}

fn sort_rows<const N: usize>(rows: &mut [SystemRow], columns: [&'static str; N]) {
    rows.sort_by(|left, right| {
        columns
            .iter()
            .map(|column| sort_key(left.get(column)))
            .collect::<Vec<_>>()
            .cmp(
                &columns
                    .iter()
                    .map(|column| sort_key(right.get(column)))
                    .collect::<Vec<_>>(),
            )
    });
}

fn sort_key(value: Option<&ScalarValue>) -> String {
    match value {
        Some(ScalarValue::Utf8(Some(value))) => value.to_ascii_lowercase(),
        Some(ScalarValue::Int32(Some(value))) => format!("{value:010}"),
        Some(ScalarValue::Int64(Some(value))) => format!("{value:020}"),
        _ => String::new(),
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

fn table_full_name(
    table: &TableInfo,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
) -> String {
    if !table.full_name.is_empty() {
        table.full_name.clone()
    } else {
        format!("{catalog_name}.{schema_name}.{table_name}")
    }
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

fn is_view_like(table: &TableInfo) -> bool {
    table
        .table_type
        .as_deref()
        .is_some_and(|table_type| table_type.eq_ignore_ascii_case("VIEW"))
}

fn is_system_catalog(catalog: &CatalogInfo) -> bool {
    catalog
        .catalog_type
        .as_deref()
        .is_some_and(|catalog_type| catalog_type.eq_ignore_ascii_case("SYSTEM_CATALOG"))
        || catalog.name.eq_ignore_ascii_case(SYSTEM_CATALOG)
}

fn to_datafusion_error(error: HarborError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
