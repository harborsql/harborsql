use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
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
        write_type_desc(writer, encoder.schema_type);
    });
    writer.write_field(T_I32, 3, |writer| writer.write_i32(position));
    writer.write_stop();
}

fn write_type_desc(writer: &mut Writer, type_id: i32) {
    writer.write_field(T_LIST, 1, |writer| {
        writer.write_list_begin(T_STRUCT, 1);
        writer.write_field(T_STRUCT, 1, |writer| {
            writer.write_field(T_I32, 1, |writer| writer.write_i32(type_id));
            writer.write_stop();
        });
        writer.write_stop();
    });
    writer.write_stop();
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
}

#[derive(Debug, Clone, Copy)]
enum PhysicalType {
    Bool,
    I32,
    I64,
    F64,
    String,
}

enum ThriftValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
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
        DataType::Int8 | DataType::Int16 | DataType::Int32 => (INT_TYPE, PhysicalType::I32),
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => (BIGINT_TYPE, PhysicalType::I64),
        DataType::Float32 | DataType::Float64 => (DOUBLE_TYPE, PhysicalType::F64),
        DataType::Utf8 | DataType::LargeUtf8 => (STRING_TYPE, PhysicalType::String),
        DataType::Date32 | DataType::Date64 => (DATE_TYPE, PhysicalType::String),
        DataType::Timestamp(_, _) => (TIMESTAMP_TYPE, PhysicalType::String),
        other => {
            return Err(HarborError::UnsupportedResultType(format!(
                "unsupported Arrow result type `{other}`"
            )));
        }
    };
    Ok(ColumnEncoder {
        schema_type,
        physical_type,
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
        other => {
            return Err(unexpected_value_type(
                other,
                "Utf8/LargeUtf8/Date/Timestamp",
            ));
        }
    };
    Ok(value)
}

fn unexpected_value_type(actual: &DataType, expected: &str) -> HarborError {
    HarborError::UnsupportedResultType(format!(
        "expected Arrow value type {expected}, got `{actual}`"
    ))
}
