use std::sync::Arc;

use datafusion::arrow::{
    array::{ArrayRef, BooleanArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::{
    engine::{Column, QueryResult},
    error::Result,
};

use super::{TableExtendedRow, TableRow};

pub(super) fn catalogs(names: Vec<String>) -> Result<QueryResult> {
    string_result("catalog", names)
}

pub(super) fn schemas(names: Vec<String>) -> Result<QueryResult> {
    string_result("databaseName", names)
}

pub(super) fn tables(rows: Vec<TableRow>) -> Result<QueryResult> {
    table_rows_result(
        vec!["database", "tableName", "isTemporary"],
        rows.into_iter()
            .map(|row| (row.schema, row.name, false))
            .collect(),
    )
}

pub(super) fn views(rows: Vec<TableRow>) -> Result<QueryResult> {
    table_rows_result(
        vec!["namespace", "viewName", "isTemporary"],
        rows.into_iter()
            .map(|row| (row.schema, row.name, false))
            .collect(),
    )
}

pub(super) fn columns(names: Vec<String>) -> Result<QueryResult> {
    string_result("col_name", names)
}

pub(super) fn table_extended(rows: Vec<TableExtendedRow>) -> Result<QueryResult> {
    let schema_names = rows
        .iter()
        .map(|row| row.schema.as_str())
        .collect::<Vec<_>>();
    let table_names = rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>();
    let information = rows
        .iter()
        .map(|row| row.information.as_str())
        .collect::<Vec<_>>();
    let is_temporary = vec![false; rows.len()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("database", DataType::Utf8, false),
        Field::new("tableName", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
        Field::new("information", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(schema_names)) as ArrayRef,
            Arc::new(StringArray::from(table_names)) as ArrayRef,
            Arc::new(BooleanArray::from(is_temporary)) as ArrayRef,
            Arc::new(StringArray::from(information)) as ArrayRef,
        ],
    )?;
    Ok(QueryResult::from_batches(
        vec![
            string_column("database"),
            string_column("tableName"),
            boolean_column("isTemporary"),
            string_column("information"),
        ],
        vec![batch],
    ))
}

fn string_result(column_name: &str, values: Vec<String>) -> Result<QueryResult> {
    let values = values.iter().map(String::as_str).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(values)) as ArrayRef],
    )?;
    Ok(QueryResult::from_batches(
        vec![string_column(column_name)],
        vec![batch],
    ))
}

fn table_rows_result(
    column_names: Vec<&'static str>,
    rows: Vec<(String, String, bool)>,
) -> Result<QueryResult> {
    let namespaces = rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>();
    let names = rows.iter().map(|row| row.1.as_str()).collect::<Vec<_>>();
    let is_temporary = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new(column_names[0], DataType::Utf8, false),
        Field::new(column_names[1], DataType::Utf8, false),
        Field::new(column_names[2], DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(namespaces)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(BooleanArray::from(is_temporary)) as ArrayRef,
        ],
    )?;
    Ok(QueryResult::from_batches(
        vec![
            string_column(column_names[0]),
            string_column(column_names[1]),
            boolean_column(column_names[2]),
        ],
        vec![batch],
    ))
}

fn string_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Utf8.to_string(),
        nullable: false,
    }
}

fn boolean_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Boolean.to_string(),
        nullable: false,
    }
}
