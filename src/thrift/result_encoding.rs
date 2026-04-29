use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};

use crate::engine::{Column, QueryResult, QueryResultPage};

use super::{codec::Writer, protocol::*, write_status};

pub(super) fn write_result_set_metadata_response(
    writer: &mut Writer,
    result: &QueryResult,
    status: i32,
) {
    writer.write_field(T_STRUCT, 1, |writer| write_status(writer, status, None));
    writer.write_field(T_STRUCT, 2, |writer| {
        write_table_schema(writer, &result.columns)
    });
    writer.write_field(T_I32, 1281, |writer| writer.write_i32(COLUMN_BASED_SET));
    writer.write_field(T_BOOL, 1282, |writer| writer.write_bool(false));
    writer.write_field(T_BOOL, 1287, |writer| writer.write_bool(false));
    writer.write_stop();
}

pub(super) fn write_result_set_metadata_response_with_error(
    writer: &mut Writer,
    status: i32,
    message: &str,
) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_status(writer, status, Some(message));
    });
    writer.write_field(T_STRUCT, 2, |writer| write_table_schema(writer, &[]));
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
) {
    let page = row_page(result, start_row_offset, max_rows);
    writer.write_field(T_STRUCT, 1, |writer| {
        write_status(writer, SUCCESS_STATUS, None)
    });
    writer.write_field(T_BOOL, 2, |writer| writer.write_bool(page.has_more_rows));
    writer.write_field(T_STRUCT, 3, |writer| {
        write_row_set(writer, result, &page);
    });
    if include_metadata {
        writer.write_field(T_STRUCT, 1281, |writer| {
            write_result_set_metadata_response(writer, result, SUCCESS_STATUS);
        });
    }
    writer.write_stop();
}

fn write_table_schema(writer: &mut Writer, columns: &[Column]) {
    writer.write_field(T_LIST, 1, |writer| {
        writer.write_list_begin(T_STRUCT, columns.len());
        for (position, column) in columns.iter().enumerate() {
            write_column_desc(writer, column, position as i32);
        }
    });
    writer.write_stop();
}

fn write_column_desc(writer: &mut Writer, column: &Column, position: i32) {
    writer.write_field(T_STRING, 1, |writer| writer.write_string(&column.name));
    writer.write_field(T_STRUCT, 2, |writer| {
        write_type_desc(writer, column_kind(column).schema_type);
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

pub(super) fn write_row_set(writer: &mut Writer, result: &QueryResult, page: &QueryResultPage) {
    writer.write_field(T_I64, 1, |writer| writer.write_i64(page.start_row_offset));
    writer.write_field(T_LIST, 3, |writer| {
        writer.write_list_begin(T_STRUCT, result.columns.len());
        for (column_index, column) in result.columns.iter().enumerate() {
            write_column(writer, column, column_index, page);
        }
    });
    writer.write_field(T_I32, 5, |writer| {
        writer.write_i32(result.columns.len() as i32)
    });
    writer.write_stop();
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

fn write_column(writer: &mut Writer, column: &Column, column_index: usize, page: &QueryResultPage) {
    match column_kind(column).physical_type {
        PhysicalType::Bool => writer.write_field(T_STRUCT, 1, |writer| {
            write_column_values(writer, T_BOOL, page, column_index, |writer, array, row| {
                writer.write_bool(arrow_value_to_bool(array.as_ref(), row));
            });
        }),
        PhysicalType::I32 => writer.write_field(T_STRUCT, 4, |writer| {
            write_column_values(writer, T_I32, page, column_index, |writer, array, row| {
                writer.write_i32(arrow_value_to_i64(array.as_ref(), row) as i32);
            });
        }),
        PhysicalType::I64 => writer.write_field(T_STRUCT, 5, |writer| {
            write_column_values(writer, T_I64, page, column_index, |writer, array, row| {
                writer.write_i64(arrow_value_to_i64(array.as_ref(), row));
            });
        }),
        PhysicalType::F64 => writer.write_field(T_STRUCT, 6, |writer| {
            write_column_values(
                writer,
                T_DOUBLE,
                page,
                column_index,
                |writer, array, row| {
                    writer.write_double(arrow_value_to_f64(array.as_ref(), row));
                },
            );
        }),
        PhysicalType::String => writer.write_field(T_STRUCT, 7, |writer| {
            write_column_values(
                writer,
                T_STRING,
                page,
                column_index,
                |writer, array, row| {
                    writer.write_string(&arrow_value_to_string(array.as_ref(), row));
                },
            );
        }),
    }
    writer.write_stop();
}

fn write_column_values<F>(
    writer: &mut Writer,
    value_type: u8,
    page: &QueryResultPage,
    column_index: usize,
    write_value: F,
) where
    F: Fn(&mut Writer, &ArrayRef, usize),
{
    let nulls = null_bitset(&page.batches, column_index, page.row_count);
    writer.write_field(T_LIST, 1, |writer| {
        writer.write_list_begin(value_type, page.row_count);
        for batch in &page.batches {
            let Some(array) = batch.columns().get(column_index) else {
                continue;
            };
            for row in 0..batch.num_rows() {
                write_value(writer, array, row);
            }
        }
    });
    writer.write_field(T_STRING, 2, |writer| writer.write_binary(&nulls));
    writer.write_stop();
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

#[derive(Debug, Clone, Copy)]
struct ColumnKind {
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

fn column_kind(column: &Column) -> ColumnKind {
    let data_type = column.data_type.to_ascii_lowercase();
    if data_type == "boolean" || data_type == "bool" {
        ColumnKind {
            schema_type: BOOLEAN_TYPE,
            physical_type: PhysicalType::Bool,
        }
    } else if data_type == "int32" || data_type == "int16" || data_type == "int8" {
        ColumnKind {
            schema_type: INT_TYPE,
            physical_type: PhysicalType::I32,
        }
    } else if data_type == "int64"
        || data_type == "uint64"
        || data_type == "uint32"
        || data_type == "uint16"
        || data_type == "uint8"
    {
        ColumnKind {
            schema_type: BIGINT_TYPE,
            physical_type: PhysicalType::I64,
        }
    } else if data_type == "float64" || data_type == "float32" {
        ColumnKind {
            schema_type: DOUBLE_TYPE,
            physical_type: PhysicalType::F64,
        }
    } else if data_type.starts_with("date") {
        ColumnKind {
            schema_type: DATE_TYPE,
            physical_type: PhysicalType::String,
        }
    } else if data_type.starts_with("timestamp") {
        ColumnKind {
            schema_type: TIMESTAMP_TYPE,
            physical_type: PhysicalType::String,
        }
    } else {
        ColumnKind {
            schema_type: STRING_TYPE,
            physical_type: PhysicalType::String,
        }
    }
}

fn arrow_value_to_bool(array: &dyn Array, row: usize) -> bool {
    if array.is_null(row) {
        return false;
    }

    match array.data_type() {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| array.value(row))
            .unwrap_or_default(),
        _ => false,
    }
}

fn arrow_value_to_i64(array: &dyn Array, row: usize) -> i64 {
    if array.is_null(row) {
        return 0;
    }

    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|array| i64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|array| i64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| i64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| array.value(row))
            .unwrap_or_default(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|array| i64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|array| i64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|array| i64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .and_then(|array| i64::try_from(array.value(row)).ok())
            .unwrap_or_default(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|array| array.value(row) as i64)
            .unwrap_or_default(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|array| array.value(row) as i64)
            .unwrap_or_default(),
        _ => 0,
    }
}

fn arrow_value_to_f64(array: &dyn Array, row: usize) -> f64 {
    if array.is_null(row) {
        return 0.0;
    }

    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|array| f64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|array| f64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| f64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| array.value(row) as f64)
            .unwrap_or_default(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|array| f64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|array| f64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|array| f64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|array| array.value(row) as f64)
            .unwrap_or_default(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|array| f64::from(array.value(row)))
            .unwrap_or_default(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|array| array.value(row))
            .unwrap_or_default(),
        _ => 0.0,
    }
}

fn arrow_value_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        String::new()
    } else {
        array_value_to_string(array, row).unwrap_or_default()
    }
}
