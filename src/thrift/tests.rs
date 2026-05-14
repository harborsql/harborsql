use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use datafusion::arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int32Builder, Int64Array,
        LargeStringArray, ListBuilder, MapBuilder, StringArray, StringBuilder, StringViewArray,
        TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};

use crate::{engine::Column, error::ClientError};

use super::codec::{MAX_CONTAINER_ELEMENTS, MAX_SKIP_DEPTH};
use super::result_encoding::{null_bitset, row_page, write_row_set};
use super::*;

const GOLDEN_OPEN_SESSION_REQUEST: &[u8] = &[
    0x80, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0b, b'O', b'p', b'e', b'n', b'S', b'e', b's', b's',
    b'i', b'o', b'n', 0x00, 0x00, 0x00, 0x07, 0x0c, 0x00, 0x01, 0x0c, 0x05, 0x04, 0x0b, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x04, b'm', b'a', b'i', b'n', 0x0b, 0x00, 0x02, 0x00, 0x00, 0x00, 0x07, b'd',
    b'e', b'f', b'a', b'u', b'l', b't', 0x00, 0x00, 0x00,
];

const GOLDEN_FETCH_RESULTS_REQUEST: &[u8] = &[
    0x80, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0c, b'F', b'e', b't', b'c', b'h', b'R', b'e', b's',
    b'u', b'l', b't', b's', 0x00, 0x00, 0x00, 0x0b, 0x0c, 0x00, 0x01, 0x0c, 0x00, 0x01, 0x0c, 0x00,
    0x01, 0x0b, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x0b, 0x00, 0x02, 0x00, 0x00, 0x00, 0x10, 0x10,
    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x00,
    0x08, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x03, 0x01, 0x00, 0x0a, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x0a, 0x05, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x01, 0x00, 0x00,
];

#[test]
fn golden_open_session_request_decodes() {
    let message = decode_message(GOLDEN_OPEN_SESSION_REQUEST).unwrap();

    assert_eq!(message.name, "OpenSession");
    assert_eq!(message.message_type, T_MESSAGE_CALL);
    assert_eq!(message.seqid, 7);

    let request = read_args(message.payload, read_open_session_req).unwrap();
    assert_eq!(request.catalog.as_deref(), Some("main"));
    assert_eq!(request.schema.as_deref(), Some("default"));
}

#[test]
fn golden_fetch_results_request_decodes_operation_handle_and_pagination() {
    let message = decode_message(GOLDEN_FETCH_RESULTS_REQUEST).unwrap();

    assert_eq!(message.name, "FetchResults");
    assert_eq!(message.message_type, T_MESSAGE_CALL);
    assert_eq!(message.seqid, 11);

    let request = read_args(message.payload, read_fetch_results_req).unwrap();
    let handle = request.operation_handle.unwrap();
    let expected_guid = Uuid::from_slice(&(0_u8..16).collect::<Vec<_>>())
        .unwrap()
        .to_string();
    let expected_secret: [u8; 16] = (16_u8..32).collect::<Vec<_>>().try_into().unwrap();

    assert_eq!(handle.id, expected_guid);
    assert_eq!(handle.secret, expected_secret);
    assert_eq!(request.start_row_offset, Some(1));
    assert_eq!(request.max_rows, Some(1));
}

#[test]
fn skip_rejects_large_containers() {
    let mut bytes = vec![T_I32];
    bytes.extend(((MAX_CONTAINER_ELEMENTS + 1) as i32).to_be_bytes());

    let err = Reader::new(&bytes).skip(T_LIST).unwrap_err().to_string();

    assert!(err.contains("size exceeds maximum"));
}

#[test]
fn skip_rejects_stop_as_container_element_type() {
    let mut bytes = vec![T_STOP];
    bytes.extend(0_i32.to_be_bytes());

    let err = Reader::new(&bytes).skip(T_LIST).unwrap_err().to_string();

    assert!(err.contains("invalid Thrift list/set element type"));
}

#[test]
fn skip_rejects_stop_as_map_key_or_value_type() {
    let mut bytes = vec![T_STOP, T_I32];
    bytes.extend(0_i32.to_be_bytes());

    let err = Reader::new(&bytes).skip(T_MAP).unwrap_err().to_string();

    assert!(err.contains("invalid Thrift map key type"));
}

#[test]
fn skip_rejects_excessive_nesting_depth() {
    let mut bytes = Vec::new();
    for _ in 0..=MAX_SKIP_DEPTH {
        bytes.push(T_LIST);
        bytes.extend(1_i32.to_be_bytes());
    }
    bytes.push(T_I32);
    bytes.extend(1_i32.to_be_bytes());
    bytes.extend(42_i32.to_be_bytes());

    let err = Reader::new(&bytes).skip(T_LIST).unwrap_err().to_string();

    assert!(err.contains("nesting depth exceeds maximum"));
}

#[tokio::test]
async fn service_handles_open_execute_status_metadata_fetch_and_close() {
    let config = test_config();
    let service = DatabricksThriftService::new(config.clone(), QueryEngine::new(config));
    let token = "test-token";

    let open_response = service
        .handle(token, &open_session_call(1, "main", "default"))
        .await
        .unwrap();
    let session = read_handle_response(&open_response, "OpenSession", 3, read_session_handle);

    let execute_response = service
        .handle(token, &execute_statement_call(2, &session, "SET"))
        .await
        .unwrap();
    let operation = read_handle_response(
        &execute_response,
        "ExecuteStatement",
        2,
        read_operation_handle,
    );

    let mut state = RUNNING_STATE;
    for _ in 0..20 {
        let status_response = service
            .handle(token, &operation_call("GetOperationStatus", 3, &operation))
            .await
            .unwrap();
        let (status_code, operation_state, has_result_set) =
            read_operation_status_response(&status_response);
        assert_eq!(status_code, SUCCESS_STATUS);
        state = operation_state;
        if state == FINISHED_STATE {
            assert!(has_result_set);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(state, FINISHED_STATE);

    let metadata_response = service
        .handle(
            token,
            &operation_call("GetResultSetMetadata", 4, &operation),
        )
        .await
        .unwrap();
    assert_eq!(
        read_top_level_status(&metadata_response, "GetResultSetMetadata"),
        SUCCESS_STATUS
    );

    let fetch_response = service
        .handle(token, &fetch_results_call(5, &operation, 0, 10))
        .await
        .unwrap();
    assert_eq!(
        read_top_level_status(&fetch_response, "FetchResults"),
        SUCCESS_STATUS
    );

    let close_operation_response = service
        .handle(token, &operation_call("CloseOperation", 6, &operation))
        .await
        .unwrap();
    assert_eq!(
        read_top_level_status(&close_operation_response, "CloseOperation"),
        SUCCESS_STATUS
    );

    let close_session_response = service
        .handle(token, &close_session_call(7, &session))
        .await
        .unwrap();
    assert_eq!(
        read_top_level_status(&close_session_response, "CloseSession"),
        SUCCESS_STATUS
    );
}

#[tokio::test]
async fn service_handles_cancellation_and_invalid_handles() {
    let config = test_config();
    let service = DatabricksThriftService::new(config.clone(), QueryEngine::new(config));
    let token = "test-token";
    let operation_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let task = tokio::spawn(futures::future::pending::<OperationCompletion>());
    let operation = Handle {
        id: operation_id.to_string(),
        secret: *secret.as_bytes(),
    };

    service.operations.write().await.insert(
        operation.id.clone(),
        OperationState {
            secret: operation.secret,
            session_id: "session".to_string(),
            token_fingerprint: token_fingerprint(token),
            state: OperationExecution::Running {
                started: Instant::now(),
                task,
            },
        },
    );

    let cancel_response = service
        .handle(token, &operation_call("CancelOperation", 1, &operation))
        .await
        .unwrap();
    assert_eq!(
        read_top_level_status(&cancel_response, "CancelOperation"),
        SUCCESS_STATUS
    );
    let stored_state = service
        .operations
        .read()
        .await
        .get(&operation.id)
        .map(|operation| operation.state.operation_state());
    assert_eq!(stored_state, Some(CANCELED_STATE));

    let invalid = Handle {
        id: Uuid::new_v4().to_string(),
        secret: *Uuid::new_v4().as_bytes(),
    };
    let invalid_response = service
        .handle(token, &operation_call("GetOperationStatus", 2, &invalid))
        .await
        .unwrap();
    assert_eq!(
        read_top_level_status(&invalid_response, "GetOperationStatus"),
        INVALID_HANDLE_STATUS
    );
}

#[test]
fn row_set_encodes_typed_value_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("u8", DataType::UInt8, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("u32", DataType::UInt32, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("large_text", DataType::LargeUtf8, true),
        Field::new("text_view", DataType::Utf8View, true),
        Field::new("date32", DataType::Date32, true),
        Field::new("date64", DataType::Date64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true)])),
            Arc::new(Int8Array::from(vec![Some(-8)])),
            Arc::new(Int16Array::from(vec![Some(-16)])),
            Arc::new(Int32Array::from(vec![Some(7)])),
            Arc::new(Int64Array::from(vec![Some(8)])),
            Arc::new(UInt8Array::from(vec![Some(9)])),
            Arc::new(UInt16Array::from(vec![Some(10)])),
            Arc::new(UInt32Array::from(vec![Some(11)])),
            Arc::new(UInt64Array::from(vec![Some(i64::MAX as u64)])),
            Arc::new(Float32Array::from(vec![Some(1.25)])),
            Arc::new(Float64Array::from(vec![Some(1.5)])),
            Arc::new(StringArray::from(vec![Some("harbor")])),
            Arc::new(LargeStringArray::from(vec![Some("harbor-large")])),
            Arc::new(StringViewArray::from(vec![Some("harbor-view")])),
            Arc::new(Date32Array::from(vec![Some(1)])),
            Arc::new(Date64Array::from(vec![Some(86_400_000)])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(
                1_700_000_000_000_000,
            )])),
        ],
    )
    .unwrap();
    let result = QueryResult::from_batches(columns_from_schema(&schema), vec![batch]);
    let page = row_page(&result, 0, Some(1));
    let mut writer = Writer::new();
    write_row_set(&mut writer, &result, &page).unwrap();

    let bytes = writer.into_inner();
    let mut reader = Reader::new(&bytes);
    let mut encoded_column_fields = Vec::new();
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        if field_id == 3 {
            assert_eq!(field_type, T_LIST);
            assert_eq!(reader.read_u8().unwrap(), T_STRUCT);
            assert_eq!(reader.read_i32().unwrap(), 17);
            for _ in 0..17 {
                let (column_type, column_field_id) = reader.read_field_begin().unwrap();
                encoded_column_fields.push((column_type, column_field_id));
                reader.skip(column_type).unwrap();
                let (stop, _) = reader.read_field_begin().unwrap();
                assert_eq!(stop, T_STOP);
            }
        } else {
            reader.skip(field_type).unwrap();
        }
    }

    assert_eq!(
        encoded_column_fields,
        vec![
            (T_STRUCT, 1),
            (T_STRUCT, 4),
            (T_STRUCT, 4),
            (T_STRUCT, 4),
            (T_STRUCT, 5),
            (T_STRUCT, 5),
            (T_STRUCT, 5),
            (T_STRUCT, 5),
            (T_STRUCT, 5),
            (T_STRUCT, 6),
            (T_STRUCT, 6),
            (T_STRUCT, 7),
            (T_STRUCT, 7),
            (T_STRUCT, 7),
            (T_STRUCT, 7),
            (T_STRUCT, 7),
            (T_STRUCT, 7)
        ]
    );
}

#[test]
fn row_set_rejects_uint64_values_outside_signed_bigint_range() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "too_large",
        DataType::UInt64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![Some(u64::MAX)]))],
    )
    .unwrap();
    let result = QueryResult::from_batches(columns_from_schema(&schema), vec![batch]);
    let page = row_page(&result, 0, Some(1));
    let mut writer = Writer::new();

    let err = write_row_set(&mut writer, &result, &page)
        .unwrap_err()
        .to_string();

    assert!(err.contains("UInt64 result value exceeds signed BIGINT range"));
}

#[test]
fn row_set_encodes_delta_type_compatibility_values() {
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    list_builder.values().append_value(1);
    list_builder.values().append_value(2);
    list_builder.append(true);
    list_builder.append(true);
    let list_array = list_builder.finish();

    let mut map_builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    map_builder.keys().append_value("b");
    map_builder.values().append_value(2);
    map_builder.keys().append_value("a");
    map_builder.values().append_value(1);
    map_builder.append(true).unwrap();
    map_builder.append(true).unwrap();
    let map_array = map_builder.finish();

    let struct_array = datafusion::arrow::array::StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("widget"), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("created", DataType::Date32, true)),
            Arc::new(Date32Array::from(vec![Some(19724), None])) as ArrayRef,
        ),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new("payload", DataType::Binary, true),
        Field::new("numbers", list_array.data_type().clone(), true),
        Field::new("attrs", map_array.data_type().clone(), true),
        Field::new("item", struct_array.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(
                Decimal128Array::from(vec![Some(123456), Some(-1234)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(BinaryArray::from(vec![
                Some(&b"\x00\x01"[..]),
                Some(&b""[..]),
            ])),
            Arc::new(list_array),
            Arc::new(map_array),
            Arc::new(struct_array),
        ],
    )
    .unwrap();
    let result = QueryResult::from_batches(columns_from_schema(&schema), vec![batch]);
    let page = row_page(&result, 0, Some(2));
    let mut writer = Writer::new();
    write_row_set(&mut writer, &result, &page).unwrap();

    let bytes = writer.into_inner();
    let mut reader = Reader::new(&bytes);
    let (_, columns) = read_row_set_values(&mut reader);

    assert_eq!(
        columns,
        vec![
            DecodedColumn::String(vec!["1234.56".to_string(), "-12.34".to_string()]),
            DecodedColumn::Binary(vec![vec![0, 1], vec![]]),
            DecodedColumn::String(vec!["[1,2]".to_string(), "[]".to_string()]),
            DecodedColumn::String(vec!["{\"a\":1,\"b\":2}".to_string(), "{}".to_string(),]),
            DecodedColumn::String(vec![
                "{\"name\":\"widget\",\"created\":2024-01-02}".to_string(),
                "{\"name\":null,\"created\":null}".to_string(),
            ]),
        ]
    );
}

#[test]
fn fetch_response_encodes_typed_values_and_pagination() {
    let result = typed_pagination_result();

    let first_response = write_fetch_results_response(21, &result, true, 0, Some(1));
    let first_page = read_fetch_page(&first_response);

    assert_eq!(first_page.status, SUCCESS_STATUS);
    assert!(first_page.has_more_rows);
    assert!(first_page.metadata_included);
    assert_eq!(first_page.start_row_offset, 0);
    assert_eq!(
        first_page.columns,
        vec![
            DecodedColumn::Bool(vec![true]),
            DecodedColumn::I32(vec![7]),
            DecodedColumn::I64(vec![9]),
            DecodedColumn::F64(vec![1.5]),
            DecodedColumn::String(vec!["alpha".to_string()]),
            DecodedColumn::String(vec!["alpha-view".to_string()]),
        ]
    );

    let second_response = write_fetch_results_response(22, &result, false, 1, Some(1));
    let second_page = read_fetch_page(&second_response);

    assert_eq!(second_page.status, SUCCESS_STATUS);
    assert!(!second_page.has_more_rows);
    assert!(!second_page.metadata_included);
    assert_eq!(second_page.start_row_offset, 1);
    assert_eq!(
        second_page.columns,
        vec![
            DecodedColumn::Bool(vec![false]),
            DecodedColumn::I32(vec![8]),
            DecodedColumn::I64(vec![10]),
            DecodedColumn::F64(vec![2.5]),
            DecodedColumn::String(vec!["beta".to_string()]),
            DecodedColumn::String(vec!["beta-view".to_string()]),
        ]
    );
}

#[test]
fn metadata_response_reports_unsupported_result_type() {
    let result = QueryResult::from_batches_with_data_types(
        vec![Column {
            name: "duration".to_string(),
            data_type: "Duration(Second)".to_string(),
            nullable: true,
        }],
        vec![DataType::Duration(TimeUnit::Second)],
        Vec::new(),
    );

    let response = write_get_result_set_metadata_response(1, &result);
    let (status, message) = read_top_level_status_and_message(&response, "GetResultSetMetadata");

    assert_eq!(status, ERROR_STATUS);
    assert_eq!(
        message.as_deref(),
        Some("UNSUPPORTED_RESULT_TYPE: unsupported result column type")
    );
}

#[test]
fn metadata_response_encodes_decimal_and_complex_type_ids() {
    let result = QueryResult::from_batches_with_data_types(
        vec![
            Column {
                name: "amount".to_string(),
                data_type: "Decimal128(10, 2)".to_string(),
                nullable: true,
            },
            Column {
                name: "numbers".to_string(),
                data_type: "List(Int32)".to_string(),
                nullable: true,
            },
            Column {
                name: "attrs".to_string(),
                data_type: "Map(Utf8, Int32)".to_string(),
                nullable: true,
            },
            Column {
                name: "item".to_string(),
                data_type: "Struct".to_string(),
                nullable: true,
            },
            Column {
                name: "payload".to_string(),
                data_type: "Binary".to_string(),
                nullable: true,
            },
        ],
        vec![
            DataType::Decimal128(10, 2),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
            DataType::Binary,
        ],
        Vec::new(),
    );

    let response = write_get_result_set_metadata_response(1, &result);
    let metadata = read_metadata_types(&response);

    assert_eq!(
        metadata,
        vec![
            DecodedType {
                type_id: DECIMAL_TYPE,
                decimal: Some((10, 2)),
            },
            DecodedType {
                type_id: ARRAY_TYPE,
                decimal: None,
            },
            DecodedType {
                type_id: MAP_TYPE,
                decimal: None,
            },
            DecodedType {
                type_id: STRUCT_TYPE,
                decimal: None,
            },
            DecodedType {
                type_id: BINARY_TYPE,
                decimal: None,
            },
        ]
    );
}

#[test]
fn row_page_respects_offset_and_limit() {
    let result = int_result(&[Some(1), Some(2), Some(3)]);

    let page = row_page(&result, 1, Some(1));

    assert_eq!(page.start_row_offset, 1);
    assert_eq!(page.row_count, 1);
    let ids = page.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 2);
    assert!(page.has_more_rows);
}

#[test]
fn row_page_clamps_negative_offset_and_large_limit() {
    let result = int_result(&[Some(1), Some(2)]);

    let page = row_page(&result, -10, Some((MAX_FETCH_ROWS + 100) as i64));

    assert_eq!(page.start_row_offset, 0);
    assert_eq!(page.row_count, 2);
    assert!(!page.has_more_rows);
}

#[test]
fn row_page_can_span_record_batches() {
    let result = int_result_from_batches(&[&[Some(1), Some(2)], &[Some(3), Some(4)]]);

    let page = row_page(&result, 1, Some(2));

    assert_eq!(page.start_row_offset, 1);
    assert_eq!(page.row_count, 2);
    assert_eq!(page.batches.len(), 2);
    let first = page.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let second = page.batches[1]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(first.value(0), 2);
    assert_eq!(second.value(0), 3);
    assert!(page.has_more_rows);
}

#[test]
fn null_bitset_reads_arrow_nulls() {
    let result = int_result(&[Some(1), None, Some(3), None, Some(5)]);
    let page = row_page(&result, 0, Some(5));

    assert_eq!(
        null_bitset(&page.batches, 0, page.row_count),
        vec![0b0000_1010]
    );
}

fn int_result(values: &[Option<i32>]) -> QueryResult {
    int_result_from_batches(&[values])
}

fn int_result_from_batches(values: &[&[Option<i32>]]) -> QueryResult {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let batches = values
        .iter()
        .map(|values| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(values.to_vec()))],
            )
            .unwrap()
        })
        .collect();
    QueryResult::from_batches(
        vec![Column {
            name: "id".to_string(),
            data_type: "Int32".to_string(),
            nullable: true,
        }],
        batches,
    )
}

fn typed_pagination_result() -> QueryResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("text_view", DataType::Utf8View, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
            Arc::new(Int32Array::from(vec![Some(7), Some(8)])),
            Arc::new(Int64Array::from(vec![Some(9), Some(10)])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5)])),
            Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])),
            Arc::new(StringViewArray::from(vec![
                Some("alpha-view"),
                Some("beta-view"),
            ])),
        ],
    )
    .unwrap();
    QueryResult::from_batches(columns_from_schema(&schema), vec![batch])
}

fn columns_from_schema(schema: &Schema) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .map(|field| Column {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect()
}

#[derive(Debug, PartialEq)]
struct DecodedFetchPage {
    status: i32,
    has_more_rows: bool,
    metadata_included: bool,
    start_row_offset: i64,
    columns: Vec<DecodedColumn>,
}

#[derive(Debug, PartialEq)]
enum DecodedColumn {
    Bool(Vec<bool>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    String(Vec<String>),
    Binary(Vec<Vec<u8>>),
}

#[derive(Clone, Copy)]
enum ColumnKind {
    Bool,
    I32,
    I64,
    F64,
    String,
    Binary,
}

#[derive(Debug, PartialEq)]
struct DecodedType {
    type_id: i32,
    decimal: Option<(i32, i32)>,
}

fn read_metadata_types(response: &[u8]) -> Vec<DecodedType> {
    let mut reader = success_reader(response, "GetResultSetMetadata");
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (2, T_STRUCT) => return read_table_schema_types(&mut reader),
            _ => reader.skip(field_type).unwrap(),
        }
    }
    panic!("GetResultSetMetadata response did not include schema")
}

fn read_table_schema_types(reader: &mut Reader<'_>) -> Vec<DecodedType> {
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_LIST) => {
                assert_eq!(reader.read_u8().unwrap(), T_STRUCT);
                let len = reader.read_i32().unwrap();
                assert!(len >= 0);
                return (0..len)
                    .map(|_| read_column_desc_type(reader))
                    .collect::<Vec<_>>();
            }
            _ => reader.skip(field_type).unwrap(),
        }
    }
    panic!("table schema did not include columns")
}

fn read_column_desc_type(reader: &mut Reader<'_>) -> DecodedType {
    let mut decoded = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (2, T_STRUCT) => decoded = Some(read_type_desc(reader)),
            _ => reader.skip(field_type).unwrap(),
        }
    }
    decoded.expect("column description did not include type")
}

fn read_type_desc(reader: &mut Reader<'_>) -> DecodedType {
    let mut decoded = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_LIST) => {
                assert_eq!(reader.read_u8().unwrap(), T_STRUCT);
                assert_eq!(reader.read_i32().unwrap(), 1);
                decoded = Some(read_type_entry(reader));
            }
            _ => reader.skip(field_type).unwrap(),
        }
    }
    decoded.expect("type description did not include type entries")
}

fn read_type_entry(reader: &mut Reader<'_>) -> DecodedType {
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => {
                let decoded = read_primitive_type_entry(reader);
                assert_eq!(reader.read_field_begin().unwrap().0, T_STOP);
                return decoded;
            }
            _ => reader.skip(field_type).unwrap(),
        }
    }
    panic!("type entry did not include primitive entry")
}

fn read_primitive_type_entry(reader: &mut Reader<'_>) -> DecodedType {
    let mut type_id = None;
    let mut decimal = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_I32) => type_id = Some(reader.read_i32().unwrap()),
            (2, T_STRUCT) => decimal = Some(read_decimal_type_qualifiers(reader)),
            _ => reader.skip(field_type).unwrap(),
        }
    }
    DecodedType {
        type_id: type_id.unwrap(),
        decimal,
    }
}

fn read_decimal_type_qualifiers(reader: &mut Reader<'_>) -> (i32, i32) {
    let mut precision = None;
    let mut scale = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_MAP) => {
                assert_eq!(reader.read_u8().unwrap(), T_STRING);
                assert_eq!(reader.read_u8().unwrap(), T_STRUCT);
                let len = reader.read_i32().unwrap();
                assert!(len >= 0);
                for _ in 0..len {
                    let key = reader.read_string().unwrap();
                    let value = read_type_qualifier_value(reader);
                    match key.as_str() {
                        "precision" => precision = Some(value),
                        "scale" => scale = Some(value),
                        _ => {}
                    }
                }
            }
            _ => reader.skip(field_type).unwrap(),
        }
    }
    (precision.unwrap(), scale.unwrap())
}

fn read_type_qualifier_value(reader: &mut Reader<'_>) -> i32 {
    let mut value = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_I32) => value = Some(reader.read_i32().unwrap()),
            _ => reader.skip(field_type).unwrap(),
        }
    }
    value.unwrap()
}

fn read_fetch_page(response: &[u8]) -> DecodedFetchPage {
    let mut reader = success_reader(response, "FetchResults");
    let mut status = None;
    let mut has_more_rows = None;
    let mut metadata_included = false;
    let mut start_row_offset = None;
    let mut columns = None;

    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => status = Some(read_status_code(&mut reader)),
            (2, T_BOOL) => has_more_rows = Some(reader.read_u8().unwrap() != 0),
            (3, T_STRUCT) => {
                let row_set = read_row_set_values(&mut reader);
                start_row_offset = Some(row_set.0);
                columns = Some(row_set.1);
            }
            (1281, T_STRUCT) => {
                metadata_included = true;
                reader.skip(field_type).unwrap();
            }
            _ => reader.skip(field_type).unwrap(),
        }
    }

    DecodedFetchPage {
        status: status.unwrap(),
        has_more_rows: has_more_rows.unwrap(),
        metadata_included,
        start_row_offset: start_row_offset.unwrap(),
        columns: columns.unwrap(),
    }
}

fn read_row_set_values(reader: &mut Reader<'_>) -> (i64, Vec<DecodedColumn>) {
    let mut start_row_offset = None;
    let mut columns = None;
    let mut column_count = None;

    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_I64) => start_row_offset = Some(reader.read_i64().unwrap()),
            (3, T_LIST) => {
                assert_eq!(reader.read_u8().unwrap(), T_STRUCT);
                let len = reader.read_i32().unwrap();
                assert!(len >= 0);
                let mut decoded = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    decoded.push(read_column(&mut *reader));
                }
                columns = Some(decoded);
            }
            (5, T_I32) => column_count = Some(reader.read_i32().unwrap()),
            _ => reader.skip(field_type).unwrap(),
        }
    }

    let columns = columns.unwrap();
    assert_eq!(column_count.unwrap() as usize, columns.len());
    (start_row_offset.unwrap(), columns)
}

fn read_column(reader: &mut Reader<'_>) -> DecodedColumn {
    let mut column = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        let kind = match (field_id, field_type) {
            (1, T_STRUCT) => ColumnKind::Bool,
            (4, T_STRUCT) => ColumnKind::I32,
            (5, T_STRUCT) => ColumnKind::I64,
            (6, T_STRUCT) => ColumnKind::F64,
            (7, T_STRUCT) => ColumnKind::String,
            (8, T_STRUCT) => ColumnKind::Binary,
            _ => {
                reader.skip(field_type).unwrap();
                continue;
            }
        };
        column = Some(read_column_values(reader, kind));
    }
    column.unwrap()
}

fn read_column_values(reader: &mut Reader<'_>, kind: ColumnKind) -> DecodedColumn {
    let mut column = None;
    let mut nulls = None;

    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_LIST) => column = Some(read_value_list(reader, kind)),
            (2, T_STRING) => nulls = Some(reader.read_binary().unwrap()),
            _ => reader.skip(field_type).unwrap(),
        }
    }

    assert_eq!(nulls.as_deref(), Some(&[0][..]));
    column.unwrap()
}

fn read_value_list(reader: &mut Reader<'_>, kind: ColumnKind) -> DecodedColumn {
    let element_type = reader.read_u8().unwrap();
    let len = reader.read_i32().unwrap();
    assert!(len >= 0);
    match kind {
        ColumnKind::Bool => {
            assert_eq!(element_type, T_BOOL);
            DecodedColumn::Bool((0..len).map(|_| reader.read_u8().unwrap() != 0).collect())
        }
        ColumnKind::I32 => {
            assert_eq!(element_type, T_I32);
            DecodedColumn::I32((0..len).map(|_| reader.read_i32().unwrap()).collect())
        }
        ColumnKind::I64 => {
            assert_eq!(element_type, T_I64);
            DecodedColumn::I64((0..len).map(|_| reader.read_i64().unwrap()).collect())
        }
        ColumnKind::F64 => {
            assert_eq!(element_type, T_DOUBLE);
            DecodedColumn::F64(
                (0..len)
                    .map(|_| f64::from_bits(reader.read_i64().unwrap() as u64))
                    .collect(),
            )
        }
        ColumnKind::String => {
            assert_eq!(element_type, T_STRING);
            DecodedColumn::String((0..len).map(|_| reader.read_string().unwrap()).collect())
        }
        ColumnKind::Binary => {
            assert_eq!(element_type, T_STRING);
            DecodedColumn::Binary((0..len).map(|_| reader.read_binary().unwrap()).collect())
        }
    }
}

#[test]
fn handle_identifier_reads_guid_and_secret() {
    let guid = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let mut writer = Writer::new();
    write_handle_identifier(&mut writer, guid.as_bytes(), secret.as_bytes());

    let bytes = writer.into_inner();
    let mut reader = Reader::new(&bytes);
    let handle = read_handle_identifier(&mut reader).unwrap().unwrap();

    assert_eq!(handle.id, guid.to_string());
    assert_eq!(handle.secret, *secret.as_bytes());
}

#[test]
fn handle_identifier_without_secret_is_invalid() {
    let guid = Uuid::new_v4();
    let mut writer = Writer::new();
    writer.write_field(T_STRING, 1, |writer| writer.write_binary(guid.as_bytes()));
    writer.write_stop();

    let bytes = writer.into_inner();
    let mut reader = Reader::new(&bytes);
    let handle = read_handle_identifier(&mut reader).unwrap();

    assert!(handle.is_none());
}

#[tokio::test]
async fn cancel_marks_running_operation_canceled() {
    let task = tokio::spawn(futures::future::pending::<OperationCompletion>());
    let mut operation = OperationState {
        secret: [0; 16],
        session_id: "session".to_string(),
        token_fingerprint: [0; 32],
        state: OperationExecution::Running {
            started: Instant::now(),
            task,
        },
    };

    operation.cancel();

    assert_eq!(operation.state.operation_state(), CANCELED_STATE);
    assert_eq!(operation.state.history_status(), "CANCELED");
}

#[test]
fn cancel_keeps_finished_operation_finished() {
    let mut operation = OperationState {
        secret: [0; 16],
        session_id: "session".to_string(),
        token_fingerprint: [0; 32],
        state: OperationExecution::Finished {
            result: Arc::new(QueryResult::empty()),
            duration_ms: 5,
            completed_at: Instant::now(),
        },
    };

    operation.cancel();

    assert_eq!(operation.state.operation_state(), FINISHED_STATE);
    assert_eq!(operation.state.history_status(), "FINISHED");
}

#[test]
fn failed_operation_reports_client_error_message() {
    let operation = OperationExecution::from_completion(OperationCompletion {
        duration_ms: 5,
        result: Err(ClientError::new("QUERY_FAILED", "query execution failed")),
    });

    assert_eq!(operation.operation_state(), ERROR_STATE);
    assert_eq!(
        operation.error_message().as_deref(),
        Some("QUERY_FAILED: query execution failed")
    );
}

#[test]
fn completed_operation_expires_after_ttl() {
    let operation = OperationState {
        secret: [0; 16],
        session_id: "session".to_string(),
        token_fingerprint: [0; 32],
        state: OperationExecution::Finished {
            result: Arc::new(QueryResult::empty()),
            duration_ms: 5,
            completed_at: Instant::now() - Duration::from_secs(30),
        },
    };

    assert!(operation.is_expired(Instant::now(), Duration::from_secs(10)));
}

#[tokio::test]
async fn running_operation_does_not_expire_by_completed_ttl() {
    let task = tokio::spawn(futures::future::pending::<OperationCompletion>());
    let operation = OperationState {
        secret: [0; 16],
        session_id: "session".to_string(),
        token_fingerprint: [0; 32],
        state: OperationExecution::Running {
            started: Instant::now() - Duration::from_secs(30),
            task,
        },
    };

    assert!(!operation.is_expired(Instant::now(), Duration::from_secs(10)));
    operation.abort();
}

fn test_config() -> Config {
    Config {
        bind_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        databricks_host: "https://example.com".to_string(),
        default_catalog: "main".to_string(),
        default_schema: "default".to_string(),
        aws_region: "us-west-2".to_string(),
        max_result_rows: Some(100),
        max_result_bytes: Some(1024 * 1024),
        unity_request_timeout: Duration::from_secs(1),
        query_timeout: Duration::from_secs(1),
        idle_session_timeout: Duration::from_secs(60),
        completed_operation_ttl: Duration::from_secs(60),
        cleanup_interval: Duration::from_secs(60),
        max_sessions: 16,
        max_operations: 16,
        request_body_limit_bytes: 1024 * 1024,
        parquet_pushdown_filters: true,
        parquet_reorder_filters: true,
        target_partitions: 2,
        skip_partial_aggregation_probe_rows_threshold: 10_000,
        skip_partial_aggregation_probe_ratio_threshold: 0.8,
        table_cache_ttl: Duration::from_secs(60),
        table_cache_max_entries: 16,
        table_cache_credential_expiry_skew: Duration::from_secs(1),
        databricks_count_star_alias_rewrite: true,
        databricks_expression_alias_rewrite: true,
        unsafe_log_sql: false,
    }
}

fn open_session_call(seqid: i32, catalog: &str, schema: &str) -> Vec<u8> {
    call("OpenSession", seqid, |writer| {
        writer.write_field(T_STRUCT, 1284, |writer| {
            write_namespace(writer, catalog, schema)
        });
    })
}

fn close_session_call(seqid: i32, session: &Handle) -> Vec<u8> {
    call("CloseSession", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_session_handle_value(writer, session)
        });
    })
}

fn execute_statement_call(seqid: i32, session: &Handle, statement: &str) -> Vec<u8> {
    call("ExecuteStatement", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_session_handle_value(writer, session)
        });
        writer.write_field(T_STRING, 2, |writer| writer.write_string(statement));
    })
}

fn operation_call(method: &str, seqid: i32, operation: &Handle) -> Vec<u8> {
    call(method, seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_operation_handle_value(writer, operation)
        });
    })
}

fn fetch_results_call(seqid: i32, operation: &Handle, start: i64, max_rows: i64) -> Vec<u8> {
    call("FetchResults", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_operation_handle_value(writer, operation)
        });
        writer.write_field(T_I64, 3, |writer| writer.write_i64(max_rows));
        writer.write_field(T_I64, 1282, |writer| writer.write_i64(start));
    })
}

fn call(method: &str, seqid: i32, write_req: impl FnOnce(&mut Writer)) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.write_message_begin(method, T_MESSAGE_CALL, seqid);
    writer.write_field(T_STRUCT, 1, |writer| {
        write_req(writer);
        writer.write_stop();
    });
    writer.write_stop();
    writer.into_inner()
}

fn write_session_handle_value(writer: &mut Writer, handle: &Handle) {
    let id = Uuid::parse_str(&handle.id).unwrap();
    write_session_handle(writer, id.as_bytes(), &handle.secret);
}

fn write_operation_handle_value(writer: &mut Writer, handle: &Handle) {
    let id = Uuid::parse_str(&handle.id).unwrap();
    write_operation_handle(writer, id.as_bytes(), &handle.secret, true);
}

fn read_handle_response(
    response: &[u8],
    method: &str,
    handle_field_id: i16,
    read_handle: fn(&mut Reader<'_>) -> Result<Option<Handle>>,
) -> Handle {
    let mut reader = success_reader(response, method);
    let mut status = None;
    let mut handle = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            status = Some(read_status_code(&mut reader));
        } else if field_id == handle_field_id && field_type == T_STRUCT {
            handle = read_handle(&mut reader).unwrap();
        } else {
            reader.skip(field_type).unwrap();
        }
    }

    assert_eq!(status, Some(SUCCESS_STATUS));
    handle.unwrap()
}

fn read_operation_status_response(response: &[u8]) -> (i32, i32, bool) {
    let mut reader = success_reader(response, "GetOperationStatus");
    let mut status = None;
    let mut state = None;
    let mut has_result_set = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => status = Some(read_status_code(&mut reader)),
            (2, T_I32) => state = Some(reader.read_i32().unwrap()),
            (9, T_BOOL) => has_result_set = Some(reader.read_u8().unwrap() != 0),
            _ => reader.skip(field_type).unwrap(),
        }
    }
    (
        status.unwrap(),
        state.unwrap_or_default(),
        has_result_set.unwrap_or(false),
    )
}

fn read_top_level_status(response: &[u8], method: &str) -> i32 {
    read_top_level_status_and_message(response, method).0
}

fn read_top_level_status_and_message(response: &[u8], method: &str) -> (i32, Option<String>) {
    let mut reader = success_reader(response, method);
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            panic!("{method} response did not include status");
        }
        if field_id == 1 && field_type == T_STRUCT {
            return read_status(&mut reader);
        }
        reader.skip(field_type).unwrap();
    }
}

fn read_status_code(reader: &mut Reader<'_>) -> i32 {
    read_status(reader).0
}

fn read_status(reader: &mut Reader<'_>) -> (i32, Option<String>) {
    let mut status = None;
    let mut message = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin().unwrap();
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_I32 {
            status = Some(reader.read_i32().unwrap());
        } else if field_id == 5 && field_type == T_STRING {
            message = Some(reader.read_string().unwrap());
        } else {
            reader.skip(field_type).unwrap();
        }
    }
    (status.unwrap(), message)
}

fn success_reader<'a>(response: &'a [u8], method: &str) -> Reader<'a> {
    let message = decode_message(response).unwrap();
    assert_eq!(message.name, method);
    assert_eq!(message.message_type, T_MESSAGE_REPLY);
    let mut reader = Reader::new(message.payload);
    let (field_type, field_id) = reader.read_field_begin().unwrap();
    assert_eq!((field_type, field_id), (T_STRUCT, 0));
    reader
}
