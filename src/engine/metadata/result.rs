use std::sync::Arc;

use datafusion::arrow::{
    array::{ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::{
    engine::{Column, QueryResult},
    error::Result,
};

use super::{ColumnMetadataRow, TableExtendedRow, TableRow};

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

pub(super) fn column_metadata(rows: Vec<ColumnMetadataRow>) -> Result<QueryResult> {
    let table_cat = required_string_values(&rows, |row| &row.catalog);
    let table_schem = required_string_values(&rows, |row| &row.schema);
    let table_name = required_string_values(&rows, |row| &row.table);
    let column_name = required_string_values(&rows, |row| &row.column);
    let data_type = required_i32_values(&rows, |row| row.data_type);
    let type_name = required_string_values(&rows, |row| &row.type_name);
    let column_size = optional_i32_values(&rows, |row| row.column_size);
    let buffer_length = vec![None::<i8>; rows.len()];
    let decimal_digits = optional_i32_values(&rows, |row| row.decimal_digits);
    let num_prec_radix = optional_i32_values(&rows, |row| row.num_prec_radix);
    let nullable = required_i32_values(&rows, |row| row.nullable);
    let remarks = optional_string_values(&rows, |row| row.remarks.as_deref());
    let column_def = vec![None::<&str>; rows.len()];
    let sql_data_type = required_i32_values(&rows, |row| row.sql_data_type);
    let sql_datetime_sub = optional_i32_values(&rows, |row| row.sql_datetime_sub);
    let char_octet_length = optional_i32_values(&rows, |row| row.char_octet_length);
    let ordinal_position = required_i32_values(&rows, |row| row.ordinal_position);
    let is_nullable = required_string_values(&rows, |row| &row.is_nullable);
    let null_strings = vec![None::<&str>; rows.len()];
    let source_data_type = vec![None::<i16>; rows.len()];
    let is_auto_increment = vec![Some("NO"); rows.len()];

    let schema = Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, false),
        Field::new("TABLE_SCHEM", DataType::Utf8, false),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("DATA_TYPE", DataType::Int32, false),
        Field::new("TYPE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_SIZE", DataType::Int32, true),
        Field::new("BUFFER_LENGTH", DataType::Int8, true),
        Field::new("DECIMAL_DIGITS", DataType::Int32, true),
        Field::new("NUM_PREC_RADIX", DataType::Int32, true),
        Field::new("NULLABLE", DataType::Int32, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("COLUMN_DEF", DataType::Utf8, true),
        Field::new("SQL_DATA_TYPE", DataType::Int32, false),
        Field::new("SQL_DATETIME_SUB", DataType::Int32, true),
        Field::new("CHAR_OCTET_LENGTH", DataType::Int32, true),
        Field::new("ORDINAL_POSITION", DataType::Int32, false),
        Field::new("IS_NULLABLE", DataType::Utf8, false),
        Field::new("SCOPE_CATALOG", DataType::Utf8, true),
        Field::new("SCOPE_SCHEMA", DataType::Utf8, true),
        Field::new("SCOPE_TABLE", DataType::Utf8, true),
        Field::new("SOURCE_DATA_TYPE", DataType::Int16, true),
        Field::new("IS_AUTO_INCREMENT", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(table_cat)) as ArrayRef,
            Arc::new(StringArray::from(table_schem)) as ArrayRef,
            Arc::new(StringArray::from(table_name)) as ArrayRef,
            Arc::new(StringArray::from(column_name)) as ArrayRef,
            Arc::new(Int32Array::from(data_type)) as ArrayRef,
            Arc::new(StringArray::from(type_name)) as ArrayRef,
            Arc::new(Int32Array::from(column_size)) as ArrayRef,
            Arc::new(Int8Array::from(buffer_length)) as ArrayRef,
            Arc::new(Int32Array::from(decimal_digits)) as ArrayRef,
            Arc::new(Int32Array::from(num_prec_radix)) as ArrayRef,
            Arc::new(Int32Array::from(nullable)) as ArrayRef,
            Arc::new(StringArray::from(remarks)) as ArrayRef,
            Arc::new(StringArray::from(column_def)) as ArrayRef,
            Arc::new(Int32Array::from(sql_data_type)) as ArrayRef,
            Arc::new(Int32Array::from(sql_datetime_sub)) as ArrayRef,
            Arc::new(Int32Array::from(char_octet_length)) as ArrayRef,
            Arc::new(Int32Array::from(ordinal_position)) as ArrayRef,
            Arc::new(StringArray::from(is_nullable)) as ArrayRef,
            Arc::new(StringArray::from(null_strings.clone())) as ArrayRef,
            Arc::new(StringArray::from(null_strings.clone())) as ArrayRef,
            Arc::new(StringArray::from(null_strings)) as ArrayRef,
            Arc::new(Int16Array::from(source_data_type)) as ArrayRef,
            Arc::new(StringArray::from(is_auto_increment)) as ArrayRef,
        ],
    )?;
    Ok(QueryResult::from_batches(
        vec![
            string_column("TABLE_CAT"),
            string_column("TABLE_SCHEM"),
            string_column("TABLE_NAME"),
            string_column("COLUMN_NAME"),
            int32_column("DATA_TYPE", false),
            string_column("TYPE_NAME"),
            int32_column("COLUMN_SIZE", true),
            int8_column("BUFFER_LENGTH"),
            int32_column("DECIMAL_DIGITS", true),
            int32_column("NUM_PREC_RADIX", true),
            int32_column("NULLABLE", false),
            nullable_string_column("REMARKS"),
            nullable_string_column("COLUMN_DEF"),
            int32_column("SQL_DATA_TYPE", false),
            int32_column("SQL_DATETIME_SUB", true),
            int32_column("CHAR_OCTET_LENGTH", true),
            int32_column("ORDINAL_POSITION", false),
            string_column("IS_NULLABLE"),
            nullable_string_column("SCOPE_CATALOG"),
            nullable_string_column("SCOPE_SCHEMA"),
            nullable_string_column("SCOPE_TABLE"),
            int16_column("SOURCE_DATA_TYPE"),
            string_column("IS_AUTO_INCREMENT"),
        ],
        vec![batch],
    ))
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

fn nullable_string_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Utf8.to_string(),
        nullable: true,
    }
}

fn boolean_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Boolean.to_string(),
        nullable: false,
    }
}

fn int8_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Int8.to_string(),
        nullable: true,
    }
}

fn int16_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Int16.to_string(),
        nullable: true,
    }
}

fn int32_column(name: &str, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Int32.to_string(),
        nullable,
    }
}

fn required_string_values<'a>(
    rows: &'a [ColumnMetadataRow],
    value: impl Fn(&'a ColumnMetadataRow) -> &'a str,
) -> Vec<Option<&'a str>> {
    rows.iter().map(|row| Some(value(row))).collect()
}

fn optional_string_values<'a>(
    rows: &'a [ColumnMetadataRow],
    value: impl Fn(&'a ColumnMetadataRow) -> Option<&'a str>,
) -> Vec<Option<&'a str>> {
    rows.iter().map(value).collect()
}

fn required_i32_values(
    rows: &[ColumnMetadataRow],
    value: impl Fn(&ColumnMetadataRow) -> i32,
) -> Vec<Option<i32>> {
    rows.iter().map(|row| Some(value(row))).collect()
}

fn optional_i32_values(
    rows: &[ColumnMetadataRow],
    value: impl Fn(&ColumnMetadataRow) -> Option<i32>,
) -> Vec<Option<i32>> {
    rows.iter().map(value).collect()
}
