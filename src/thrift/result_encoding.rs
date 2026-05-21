use datafusion::arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
        FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray,
        StringArray, StringViewArray, StructArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
    temporal_conversions::{
        date32_to_datetime, date64_to_datetime, timestamp_ms_to_datetime, timestamp_ns_to_datetime,
        timestamp_s_to_datetime, timestamp_us_to_datetime,
    },
    util::display::array_value_to_string,
};

use crate::{
    engine::{Column, QueryResult, QueryResultPage},
    error::{HarborError, Result},
};

use super::{codec::Writer, protocol::*, write_status};

pub(super) fn write_result_set_metadata_response(
    writer: &mut Writer,
    result: &QueryResult,
    status: i32,
) -> Result<()> {
    let encoders = column_encoders(result)?;
    write_result_set_metadata_response_with_encoders(writer, &result.columns, &encoders, status);
    Ok(())
}

pub(super) fn write_result_set_metadata_response_with_error(
    writer: &mut Writer,
    status: i32,
    message: &str,
) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_status(writer, status, Some(message));
    });
    writer.write_field(T_STRUCT, 2, |writer| write_table_schema(writer, &[], &[]));
    writer.write_field(T_I32, 1281, |writer| writer.write_i32(COLUMN_BASED_SET));
    writer.write_field(T_BOOL, 1282, |writer| writer.write_bool(false));
    writer.write_stop();
}

pub(super) fn write_fetch_results_response_struct(
    writer: &mut Writer,
    result: &QueryResult,
    include_metadata: bool,
    start_row_offset: i64,
    max_rows: Option<i64>,
) -> Result<()> {
    let page = row_page(result, start_row_offset, max_rows);
    let encoders = column_encoders(result)?;

    let mut row_writer = Writer::new();
    write_row_set_with_encoders(&mut row_writer, result, &page, &encoders)?;
    let row_set = row_writer.into_inner();

    let metadata = if include_metadata {
        let mut metadata_writer = Writer::new();
        write_result_set_metadata_response_with_encoders(
            &mut metadata_writer,
            &result.columns,
            &encoders,
            SUCCESS_STATUS,
        );
        Some(metadata_writer.into_inner())
    } else {
        None
    };

    writer.write_field(T_STRUCT, 1, |writer| {
        write_status(writer, SUCCESS_STATUS, None)
    });
    writer.write_field(T_BOOL, 2, |writer| writer.write_bool(page.has_more_rows));
    writer.write_field(T_STRUCT, 3, |writer| writer.write_raw(&row_set));
    if let Some(metadata) = metadata {
        writer.write_field(T_STRUCT, 1281, |writer| writer.write_raw(&metadata));
    }
    writer.write_stop();
    Ok(())
}

fn write_result_set_metadata_response_with_encoders(
    writer: &mut Writer,
    columns: &[Column],
    encoders: &[ColumnEncoder],
    status: i32,
) {
    writer.write_field(T_STRUCT, 1, |writer| write_status(writer, status, None));
    writer.write_field(T_STRUCT, 2, |writer| {
        write_table_schema(writer, columns, encoders)
    });
    writer.write_field(T_I32, 1281, |writer| writer.write_i32(COLUMN_BASED_SET));
    writer.write_field(T_BOOL, 1282, |writer| writer.write_bool(false));
    writer.write_field(T_BOOL, 1287, |writer| writer.write_bool(false));
    writer.write_stop();
}

fn write_table_schema(writer: &mut Writer, columns: &[Column], encoders: &[ColumnEncoder]) {
    writer.write_field(T_LIST, 1, |writer| {
        writer.write_list_begin(T_STRUCT, columns.len());
        for (position, (column, encoder)) in columns.iter().zip(encoders).enumerate() {
            write_column_desc(writer, column, encoder, position as i32);
        }
    });
    writer.write_stop();
}

fn write_column_desc(writer: &mut Writer, column: &Column, encoder: &ColumnEncoder, position: i32) {
    writer.write_field(T_STRING, 1, |writer| writer.write_string(&column.name));
    writer.write_field(T_STRUCT, 2, |writer| {
        write_type_desc(writer, encoder);
    });
    writer.write_field(T_I32, 3, |writer| writer.write_i32(position));
    writer.write_stop();
}

fn write_type_desc(writer: &mut Writer, encoder: &ColumnEncoder) {
    writer.write_field(T_LIST, 1, |writer| {
        writer.write_list_begin(T_STRUCT, 1);
        writer.write_field(T_STRUCT, 1, |writer| {
            writer.write_field(T_I32, 1, |writer| writer.write_i32(encoder.schema_type));
            if let Some((precision, scale)) = encoder.decimal {
                write_decimal_type_qualifiers(writer, precision, scale);
            }
            writer.write_stop();
        });
        writer.write_stop();
    });
    writer.write_stop();
}

fn write_decimal_type_qualifiers(writer: &mut Writer, precision: i32, scale: i32) {
    writer.write_field(T_STRUCT, 2, |writer| {
        writer.write_field(T_MAP, 1, |writer| {
            writer.write_raw(&[T_STRING, T_STRUCT]);
            writer.write_i32(2);
            writer.write_string("precision");
            writer.write_field(T_I32, 1, |writer| writer.write_i32(precision));
            writer.write_stop();
            writer.write_string("scale");
            writer.write_field(T_I32, 1, |writer| writer.write_i32(scale));
            writer.write_stop();
        });
        writer.write_stop();
    });
}

#[cfg(test)]
pub(super) fn write_row_set(
    writer: &mut Writer,
    result: &QueryResult,
    page: &QueryResultPage,
) -> Result<()> {
    let encoders = column_encoders(result)?;
    write_row_set_with_encoders(writer, result, page, &encoders)
}

fn write_row_set_with_encoders(
    writer: &mut Writer,
    result: &QueryResult,
    page: &QueryResultPage,
    encoders: &[ColumnEncoder],
) -> Result<()> {
    writer.write_field(T_I64, 1, |writer| writer.write_i64(page.start_row_offset));
    writer.write_field(T_LIST, 2, |writer| {
        writer.write_list_begin(T_STRUCT, 0);
    });
    writer.write_field_result(T_LIST, 3, |writer| {
        writer.write_list_begin(T_STRUCT, result.columns.len());
        for (column_index, encoder) in encoders.iter().enumerate() {
            write_column(writer, encoder, column_index, page)?;
        }
        Ok(())
    })?;
    writer.write_field(T_I32, 5, |writer| {
        writer.write_i32(result.columns.len() as i32)
    });
    writer.write_stop();
    Ok(())
}

pub(super) fn row_page(
    result: &QueryResult,
    start_row_offset: i64,
    max_rows: Option<i64>,
) -> QueryResultPage {
    result.page(start_row_offset, requested_row_limit(max_rows))
}

fn requested_row_limit(max_rows: Option<i64>) -> usize {
    match max_rows {
        Some(value) if value <= 0 => 0,
        Some(value) => usize::try_from(value)
            .unwrap_or(MAX_FETCH_ROWS)
            .min(MAX_FETCH_ROWS),
        None => DEFAULT_FETCH_ROWS,
    }
}

fn write_column(
    writer: &mut Writer,
    encoder: &ColumnEncoder,
    column_index: usize,
    page: &QueryResultPage,
) -> Result<()> {
    match encoder.physical_type {
        PhysicalType::Bool => writer.write_field_result(T_STRUCT, 1, |writer| {
            write_column_values(writer, T_BOOL, page, column_index, |array, row| {
                Ok(ThriftValue::Bool(arrow_value_to_bool(array.as_ref(), row)?))
            })
        })?,
        PhysicalType::I32 => writer.write_field_result(T_STRUCT, 4, |writer| {
            write_column_values(writer, T_I32, page, column_index, |array, row| {
                Ok(ThriftValue::I32(arrow_value_to_i32(array.as_ref(), row)?))
            })
        })?,
        PhysicalType::I64 => writer.write_field_result(T_STRUCT, 5, |writer| {
            write_column_values(writer, T_I64, page, column_index, |array, row| {
                Ok(ThriftValue::I64(arrow_value_to_i64(array.as_ref(), row)?))
            })
        })?,
        PhysicalType::F64 => writer.write_field_result(T_STRUCT, 6, |writer| {
            write_column_values(writer, T_DOUBLE, page, column_index, |array, row| {
                Ok(ThriftValue::F64(arrow_value_to_f64(array.as_ref(), row)?))
            })
        })?,
        PhysicalType::String => writer.write_field_result(T_STRUCT, 7, |writer| {
            write_column_values(writer, T_STRING, page, column_index, |array, row| {
                Ok(ThriftValue::String(arrow_value_to_string(
                    array.as_ref(),
                    row,
                )?))
            })
        })?,
        PhysicalType::Binary => writer.write_field_result(T_STRUCT, 8, |writer| {
            write_column_values(writer, T_STRING, page, column_index, |array, row| {
                Ok(ThriftValue::Binary(arrow_value_to_binary(
                    array.as_ref(),
                    row,
                )?))
            })
        })?,
    }
    writer.write_stop();
    Ok(())
}

fn write_column_values<F>(
    writer: &mut Writer,
    value_type: u8,
    page: &QueryResultPage,
    column_index: usize,
    read_value: F,
) -> Result<()>
where
    F: Fn(&ArrayRef, usize) -> Result<ThriftValue>,
{
    let nulls = null_bitset(&page.batches, column_index, page.row_count);
    writer.write_field_result(T_LIST, 1, |writer| {
        writer.write_list_begin(value_type, page.row_count);
        for batch in &page.batches {
            let array = batch.columns().get(column_index).ok_or_else(|| {
                HarborError::UnsupportedResultType(format!(
                    "result page is missing column index {column_index}"
                ))
            })?;
            for row in 0..batch.num_rows() {
                match read_value(array, row)? {
                    ThriftValue::Bool(value) => writer.write_bool(value),
                    ThriftValue::I32(value) => writer.write_i32(value),
                    ThriftValue::I64(value) => writer.write_i64(value),
                    ThriftValue::F64(value) => writer.write_double(value),
                    ThriftValue::String(value) => writer.write_string(&value),
                    ThriftValue::Binary(value) => writer.write_binary(&value),
                }
            }
        }
        Ok(())
    })?;
    writer.write_field(T_STRING, 2, |writer| writer.write_binary(&nulls));
    writer.write_stop();
    Ok(())
}

pub(super) fn null_bitset(
    batches: &[RecordBatch],
    column_index: usize,
    row_count: usize,
) -> Vec<u8> {
    let mut nulls = vec![0_u8; row_count.div_ceil(8)];
    let mut page_row = 0;
    for batch in batches {
        let Some(array) = batch.columns().get(column_index) else {
            continue;
        };
        for row in 0..batch.num_rows() {
            if array.is_null(row) {
                nulls[page_row >> 3] |= 1 << (page_row & 7);
            }
            page_row += 1;
        }
    }
    nulls
}

#[derive(Debug, Clone)]
struct ColumnEncoder {
    schema_type: i32,
    physical_type: PhysicalType,
    decimal: Option<(i32, i32)>,
}

#[derive(Debug, Clone, Copy)]
enum PhysicalType {
    Bool,
    I32,
    I64,
    F64,
    String,
    Binary,
}

enum ThriftValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    Binary(Vec<u8>),
}

fn column_encoders(result: &QueryResult) -> Result<Vec<ColumnEncoder>> {
    if result.columns.len() != result.data_types().len() {
        return Err(HarborError::UnsupportedResultType(format!(
            "result has {} columns but {} Arrow data types",
            result.columns.len(),
            result.data_types().len()
        )));
    }

    result
        .data_types()
        .iter()
        .map(column_encoder)
        .collect::<Result<Vec<_>>>()
}

fn column_encoder(data_type: &DataType) -> Result<ColumnEncoder> {
    let (schema_type, physical_type) = match data_type {
        DataType::Boolean => (BOOLEAN_TYPE, PhysicalType::Bool),
        DataType::Int8 => (TINYINT_TYPE, PhysicalType::I32),
        DataType::Int16 => (SMALLINT_TYPE, PhysicalType::I32),
        DataType::Int32 => (INT_TYPE, PhysicalType::I32),
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => (BIGINT_TYPE, PhysicalType::I64),
        DataType::Float32 => (FLOAT_TYPE, PhysicalType::F64),
        DataType::Float64 => (DOUBLE_TYPE, PhysicalType::F64),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            (STRING_TYPE, PhysicalType::String)
        }
        DataType::Date32 | DataType::Date64 => (DATE_TYPE, PhysicalType::String),
        DataType::Timestamp(_, _) => (TIMESTAMP_TYPE, PhysicalType::String),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            (BINARY_TYPE, PhysicalType::Binary)
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            (DECIMAL_TYPE, PhysicalType::String)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => (ARRAY_TYPE, PhysicalType::String),
        DataType::Map(_, _) => (MAP_TYPE, PhysicalType::String),
        DataType::Struct(_) => (STRUCT_TYPE, PhysicalType::String),
        other => {
            return Err(HarborError::UnsupportedResultType(format!(
                "unsupported Arrow result type `{other}`"
            )));
        }
    };
    let decimal = match data_type {
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Some((i32::from(*precision), i32::from(*scale)))
        }
        _ => None,
    };
    Ok(ColumnEncoder {
        schema_type,
        physical_type,
        decimal,
    })
}

fn arrow_value_to_bool(array: &dyn Array, row: usize) -> Result<bool> {
    if array.is_null(row) {
        return Ok(false);
    }

    let value = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| unexpected_value_type(array.data_type(), "Boolean"))?
        .value(row);
    Ok(value)
}

fn arrow_value_to_i32(array: &dyn Array, row: usize) -> Result<i32> {
    if array.is_null(row) {
        return Ok(0);
    }

    let value = match array.data_type() {
        DataType::Int8 => i32::from(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Int8"))?
                .value(row),
        ),
        DataType::Int16 => i32::from(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Int16"))?
                .value(row),
        ),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "Int32"))?
            .value(row),
        other => return Err(unexpected_value_type(other, "Int8/Int16/Int32")),
    };
    Ok(value)
}

fn arrow_value_to_i64(array: &dyn Array, row: usize) -> Result<i64> {
    if array.is_null(row) {
        return Ok(0);
    }

    let value = match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "Int64"))?
            .value(row),
        DataType::UInt8 => i64::from(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "UInt8"))?
                .value(row),
        ),
        DataType::UInt16 => i64::from(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "UInt16"))?
                .value(row),
        ),
        DataType::UInt32 => i64::from(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "UInt32"))?
                .value(row),
        ),
        DataType::UInt64 => {
            let value = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "UInt64"))?
                .value(row);
            i64::try_from(value).map_err(|_| {
                HarborError::UnsupportedResultType(
                    "UInt64 result value exceeds signed BIGINT range".into(),
                )
            })?
        }
        other => {
            return Err(unexpected_value_type(
                other,
                "Int64/UInt8/UInt16/UInt32/UInt64",
            ));
        }
    };
    Ok(value)
}

fn arrow_value_to_f64(array: &dyn Array, row: usize) -> Result<f64> {
    if array.is_null(row) {
        return Ok(0.0);
    }

    let value = match array.data_type() {
        DataType::Float32 => f64::from(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Float32"))?
                .value(row),
        ),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "Float64"))?
            .value(row),
        other => return Err(unexpected_value_type(other, "Float32/Float64")),
    };
    Ok(value)
}

fn arrow_value_to_string(array: &dyn Array, row: usize) -> Result<String> {
    if array.is_null(row) {
        return Ok(String::new());
    }

    let value = match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "Utf8"))?
            .value(row)
            .to_string(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "LargeUtf8"))?
            .value(row)
            .to_string(),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "Utf8View"))?
            .value(row)
            .to_string(),
        DataType::Date32 => {
            let _ = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Date32"))?;
            array_value_to_string(array, row).map_err(|err| {
                HarborError::UnsupportedResultType(format!("failed to format Date32 value: {err}"))
            })?
        }
        DataType::Date64 => {
            let _ = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Date64"))?;
            array_value_to_string(array, row).map_err(|err| {
                HarborError::UnsupportedResultType(format!("failed to format Date64 value: {err}"))
            })?
        }
        DataType::Timestamp(_, _) => array_value_to_string(array, row).map_err(|err| {
            HarborError::UnsupportedResultType(format!("failed to format Timestamp value: {err}"))
        })?,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            databricks_display_value(array, row)?
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _)
        | DataType::Struct(_) => databricks_display_value(array, row)?,
        other => {
            return Err(unexpected_value_type(
                other,
                "Utf8/LargeUtf8/Utf8View/Date/Timestamp/Decimal/List/Map/Struct",
            ));
        }
    };
    Ok(value)
}

fn arrow_value_to_binary(array: &dyn Array, row: usize) -> Result<Vec<u8>> {
    if array.is_null(row) {
        return Ok(Vec::new());
    }

    let value = match array.data_type() {
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "Binary"))?
            .value(row)
            .to_vec(),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "LargeBinary"))?
            .value(row)
            .to_vec(),
        DataType::FixedSizeBinary(_) => array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| unexpected_value_type(array.data_type(), "FixedSizeBinary"))?
            .value(row)
            .to_vec(),
        other => {
            return Err(unexpected_value_type(
                other,
                "Binary/LargeBinary/FixedSizeBinary",
            ));
        }
    };
    Ok(value)
}

fn databricks_display_value(array: &dyn Array, row: usize) -> Result<String> {
    if array.is_null(row) {
        return Ok("null".to_string());
    }

    let value = match array.data_type() {
        DataType::Boolean => arrow_value_to_bool(array, row)?.to_string(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            arrow_value_to_i32(array, row)?.to_string()
        }
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => arrow_value_to_i64(array, row)?.to_string(),
        DataType::Float32 | DataType::Float64 => arrow_value_to_f64(array, row)?.to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            serde_json::to_string(&arrow_value_to_string(array, row)?)?
        }
        DataType::Date32 | DataType::Date64 => format_date_value(array, row)?,
        DataType::Timestamp(_, _) => format_timestamp_value(array, row)?,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            array_value_to_string(array, row).map_err(|err| {
                HarborError::UnsupportedResultType(format!("failed to format Decimal value: {err}"))
            })?
        }
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            serde_json::to_string(&arrow_value_to_binary(array, row)?)?
        }
        DataType::List(_) => {
            let array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "List"))?;
            databricks_display_list(array.value(row).as_ref())?
        }
        DataType::LargeList(_) => {
            let array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "LargeList"))?;
            databricks_display_list(array.value(row).as_ref())?
        }
        DataType::FixedSizeList(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "FixedSizeList"))?;
            databricks_display_list(array.value(row).as_ref())?
        }
        DataType::Map(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Map"))?;
            databricks_display_map(&array.value(row))?
        }
        DataType::Struct(_) => {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Struct"))?;
            databricks_display_struct(array, row)?
        }
        other => {
            return Err(unexpected_value_type(
                other,
                "Databricks display-compatible Arrow type",
            ));
        }
    };
    Ok(value)
}

fn databricks_display_list(values: &dyn Array) -> Result<String> {
    let values = (0..values.len())
        .map(|row| databricks_display_value(values, row))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("[{}]", values.join(",")))
}

fn databricks_display_map(entries: &StructArray) -> Result<String> {
    let keys = entries.column(0);
    let values = entries.column(1);
    let mut pairs = (0..entries.len())
        .map(|row| {
            let key = if keys.is_null(row) {
                "null".to_string()
            } else {
                match keys.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        arrow_value_to_string(keys.as_ref(), row)?
                    }
                    _ => databricks_display_value(keys.as_ref(), row)?,
                }
            };
            let value = databricks_display_value(values.as_ref(), row)?;
            Ok((key, value))
        })
        .collect::<Result<Vec<_>>>()?;
    pairs.sort_by(|left, right| left.0.cmp(&right.0));

    let rendered = pairs
        .into_iter()
        .map(|(key, value)| Ok(format!("{}:{}", serde_json::to_string(&key)?, value)))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("{{{}}}", rendered.join(",")))
}

fn databricks_display_struct(array: &StructArray, row: usize) -> Result<String> {
    let rendered = array
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Ok(format!(
                "{}:{}",
                serde_json::to_string(field.name())?,
                databricks_display_value(array.column(index).as_ref(), row)?
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("{{{}}}", rendered.join(",")))
}

fn format_date_value(array: &dyn Array, row: usize) -> Result<String> {
    let value = match array.data_type() {
        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Date32"))?
                .value(row);
            date32_to_datetime(days)
        }
        DataType::Date64 => {
            let millis = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "Date64"))?
                .value(row);
            date64_to_datetime(millis)
        }
        other => return Err(unexpected_value_type(other, "Date32/Date64")),
    };
    value
        .map(|date_time| date_time.date().to_string())
        .ok_or_else(|| {
            HarborError::UnsupportedResultType(format!(
                "date value at row {row} is outside supported range"
            ))
        })
}

fn format_timestamp_value(array: &dyn Array, row: usize) -> Result<String> {
    let value = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let seconds = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "TimestampSecond"))?
                .value(row);
            timestamp_s_to_datetime(seconds)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let millis = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "TimestampMillisecond"))?
                .value(row);
            timestamp_ms_to_datetime(millis)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "TimestampMicrosecond"))?
                .value(row);
            timestamp_us_to_datetime(micros)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| unexpected_value_type(array.data_type(), "TimestampNanosecond"))?
                .value(row);
            timestamp_ns_to_datetime(nanos)
        }
        other => return Err(unexpected_value_type(other, "Timestamp")),
    };
    value.map(|date_time| date_time.to_string()).ok_or_else(|| {
        HarborError::UnsupportedResultType(format!(
            "timestamp value at row {row} is outside supported range"
        ))
    })
}

fn unexpected_value_type(actual: &DataType, expected: &str) -> HarborError {
    HarborError::UnsupportedResultType(format!(
        "expected Arrow value type {expected}, got `{actual}`"
    ))
}
