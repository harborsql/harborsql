use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::Value;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    config::Config,
    engine::{Column, QueryEngine, QueryResult},
    error::{HarborError, Result},
};

const T_STOP: u8 = 0;
const T_BOOL: u8 = 2;
const T_BYTE: u8 = 3;
const T_DOUBLE: u8 = 4;
const T_I16: u8 = 6;
const T_I32: u8 = 8;
const T_I64: u8 = 10;
const T_STRING: u8 = 11;
const T_STRUCT: u8 = 12;
const T_MAP: u8 = 13;
const T_SET: u8 = 14;
const T_LIST: u8 = 15;

const T_MESSAGE_CALL: u8 = 1;
const T_MESSAGE_REPLY: u8 = 2;
const T_MESSAGE_EXCEPTION: u8 = 3;
const T_BINARY_VERSION_1: u32 = 0x8001_0000;

const SPARK_CLI_SERVICE_PROTOCOL_V7: i32 = 42247;
const SUCCESS_STATUS: i32 = 0;
const ERROR_STATUS: i32 = 3;
const INVALID_HANDLE_STATUS: i32 = 4;
const FINISHED_STATE: i32 = 2;
const EXECUTE_STATEMENT: i32 = 0;
const COLUMN_BASED_SET: i32 = 1;

const BOOLEAN_TYPE: i32 = 0;
const INT_TYPE: i32 = 3;
const BIGINT_TYPE: i32 = 4;
const DOUBLE_TYPE: i32 = 6;
const STRING_TYPE: i32 = 7;
const TIMESTAMP_TYPE: i32 = 8;
const DATE_TYPE: i32 = 17;

#[derive(Clone)]
pub struct DatabricksThriftService {
    config: Config,
    engine: QueryEngine,
    sessions: Arc<RwLock<HashMap<String, SessionState>>>,
    operations: Arc<RwLock<HashMap<String, OperationState>>>,
}

impl DatabricksThriftService {
    pub fn new(config: Config, engine: QueryEngine) -> Self {
        Self {
            config,
            engine,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            operations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn handle(&self, bearer_token: &str, body: &[u8]) -> Result<Vec<u8>> {
        let message = decode_message(body)?;
        if message.message_type != T_MESSAGE_CALL {
            return Ok(write_application_exception(
                &message.name,
                message.seqid,
                "expected a Thrift CALL message",
            ));
        }

        match message.name.as_str() {
            "OpenSession" => {
                let request = read_args(&message.payload, read_open_session_req)?;
                let session_id = Uuid::new_v4();
                let secret = Uuid::new_v4();
                let catalog = request
                    .catalog
                    .unwrap_or_else(|| self.config.default_catalog.clone());
                let schema = request
                    .schema
                    .unwrap_or_else(|| self.config.default_schema.clone());

                self.sessions.write().await.insert(
                    session_id.to_string(),
                    SessionState {
                        catalog: catalog.clone(),
                        schema: schema.clone(),
                    },
                );

                Ok(write_open_session_response(
                    message.seqid,
                    session_id.as_bytes(),
                    secret.as_bytes(),
                    &catalog,
                    &schema,
                ))
            }
            "CloseSession" => {
                let request = read_args(&message.payload, read_close_session_req)?;
                if let Some(session_id) = request.session_id {
                    self.sessions.write().await.remove(&session_id);
                }
                Ok(write_close_session_response(message.seqid))
            }
            "ExecuteStatement" => {
                let request = read_args(&message.payload, read_execute_statement_req)?;
                let session = self.session_for(request.session_id.as_deref()).await;
                let operation_id = Uuid::new_v4();
                let secret = Uuid::new_v4();
                let statement = request.statement.trim().to_string();

                let started = Instant::now();
                let result = if is_noop_statement(&statement) {
                    Ok(QueryResult::empty())
                } else {
                    self.engine
                        .execute(bearer_token, &statement, &session.catalog, &session.schema)
                        .await
                };
                let duration_ms = started.elapsed().as_millis() as u64;

                match result {
                    Ok(result) => {
                        let query_id = operation_id.to_string();
                        self.operations.write().await.insert(
                            query_id.clone(),
                            OperationState {
                                result: result.clone(),
                                duration_ms,
                                has_result_set: true,
                                closed: false,
                            },
                        );
                        Ok(write_execute_statement_response(
                            message.seqid,
                            operation_id.as_bytes(),
                            secret.as_bytes(),
                            &result,
                        ))
                    }
                    Err(err) => Ok(write_execute_statement_error(
                        message.seqid,
                        operation_id.as_bytes(),
                        secret.as_bytes(),
                        &err.to_string(),
                    )),
                }
            }
            "GetOperationStatus" => {
                let request = read_args(&message.payload, read_operation_req)?;
                let operation = self.operation_for(request.operation_id.as_deref()).await;
                Ok(write_get_operation_status_response(
                    message.seqid,
                    operation.as_ref().map(|op| op.has_result_set),
                    operation.is_some(),
                ))
            }
            "GetResultSetMetadata" => {
                let request = read_args(&message.payload, read_operation_req)?;
                let operation = self.operation_for(request.operation_id.as_deref()).await;
                Ok(match operation {
                    Some(operation) => {
                        write_get_result_set_metadata_response(message.seqid, &operation.result)
                    }
                    None => write_get_result_set_metadata_invalid(message.seqid),
                })
            }
            "FetchResults" => {
                let request = read_args(&message.payload, read_fetch_results_req)?;
                let operation = self.operation_for(request.operation_id.as_deref()).await;
                Ok(match operation {
                    Some(operation) => write_fetch_results_response(
                        message.seqid,
                        &operation.result,
                        true,
                        request.start_row_offset.unwrap_or(0),
                    ),
                    None => write_fetch_results_invalid(message.seqid),
                })
            }
            "CloseOperation" => {
                let request = read_args(&message.payload, read_operation_req)?;
                if let Some(operation_id) = request.operation_id {
                    if let Some(operation) = self.operations.write().await.get_mut(&operation_id) {
                        operation.closed = true;
                    }
                }
                Ok(write_close_operation_response(message.seqid))
            }
            "CancelOperation" => {
                let request = read_args(&message.payload, read_operation_req)?;
                if let Some(operation_id) = request.operation_id {
                    if let Some(operation) = self.operations.write().await.get_mut(&operation_id) {
                        operation.closed = true;
                    }
                }
                Ok(write_cancel_operation_response(message.seqid))
            }
            other => Ok(write_application_exception(
                other,
                message.seqid,
                &format!("unsupported Thrift method `{other}`"),
            )),
        }
    }

    pub async fn query_history(&self, query_id: &str) -> Option<QueryHistory> {
        self.operations
            .read()
            .await
            .get(query_id)
            .map(|operation| QueryHistory {
                query_id: query_id.to_string(),
                status: if operation.closed {
                    "CLOSED".to_string()
                } else {
                    "FINISHED".to_string()
                },
                duration: operation.duration_ms,
            })
    }

    async fn session_for(&self, session_id: Option<&str>) -> SessionState {
        if let Some(session_id) = session_id {
            if let Some(session) = self.sessions.read().await.get(session_id) {
                return session.clone();
            }
        }
        SessionState {
            catalog: self.config.default_catalog.clone(),
            schema: self.config.default_schema.clone(),
        }
    }

    async fn operation_for(&self, operation_id: Option<&str>) -> Option<OperationState> {
        let operation_id = operation_id?;
        self.operations.read().await.get(operation_id).cloned()
    }
}

#[derive(Debug, Serialize)]
pub struct QueryHistory {
    pub query_id: String,
    pub status: String,
    pub duration: u64,
}

#[derive(Debug, Clone)]
struct SessionState {
    catalog: String,
    schema: String,
}

#[derive(Debug, Clone)]
struct OperationState {
    result: QueryResult,
    duration_ms: u64,
    has_result_set: bool,
    closed: bool,
}

#[derive(Debug)]
struct Message<'a> {
    name: String,
    message_type: u8,
    seqid: i32,
    payload: &'a [u8],
}

#[derive(Debug)]
struct OpenSessionReq {
    catalog: Option<String>,
    schema: Option<String>,
}

#[derive(Debug)]
struct CloseSessionReq {
    session_id: Option<String>,
}

#[derive(Debug)]
struct ExecuteStatementReq {
    session_id: Option<String>,
    statement: String,
}

#[derive(Debug)]
struct OperationReq {
    operation_id: Option<String>,
}

#[derive(Debug)]
struct FetchResultsReq {
    operation_id: Option<String>,
    start_row_offset: Option<i64>,
}

fn decode_message(body: &[u8]) -> Result<Message<'_>> {
    let mut reader = Reader::new(body);
    let version = reader.read_i32()? as u32;
    if version & 0xffff_0000 != T_BINARY_VERSION_1 {
        return Err(HarborError::Thrift(
            "only strict binary Thrift messages are supported".into(),
        ));
    }
    let message_type = (version & 0x0000_00ff) as u8;
    let name = reader.read_string()?;
    let seqid = reader.read_i32()?;
    let payload = &body[reader.position()..];
    Ok(Message {
        name,
        message_type,
        seqid,
        payload,
    })
}

fn read_args<T>(payload: &[u8], parse_req: fn(&mut Reader<'_>) -> Result<T>) -> Result<T> {
    let mut reader = Reader::new(payload);
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            let request = parse_req(&mut reader)?;
            reader.skip_remaining_struct_fields()?;
            return Ok(request);
        }
        reader.skip(field_type)?;
    }

    Err(HarborError::Thrift(
        "Thrift RPC arguments did not include field `req`".into(),
    ))
}

fn read_open_session_req(reader: &mut Reader<'_>) -> Result<OpenSessionReq> {
    let mut catalog = None;
    let mut schema = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1284 && field_type == T_STRUCT {
            let namespace = read_namespace(reader)?;
            catalog = namespace.0;
            schema = namespace.1;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(OpenSessionReq { catalog, schema })
}

fn read_namespace(reader: &mut Reader<'_>) -> Result<(Option<String>, Option<String>)> {
    let mut catalog = None;
    let mut schema = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRING) => catalog = Some(reader.read_string()?),
            (2, T_STRING) => schema = Some(reader.read_string()?),
            _ => reader.skip(field_type)?,
        }
    }
    Ok((catalog, schema))
}

fn read_close_session_req(reader: &mut Reader<'_>) -> Result<CloseSessionReq> {
    let mut session_id = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            session_id = read_session_handle(reader)?;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(CloseSessionReq { session_id })
}

fn read_execute_statement_req(reader: &mut Reader<'_>) -> Result<ExecuteStatementReq> {
    let mut session_id = None;
    let mut statement = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => session_id = read_session_handle(reader)?,
            (2, T_STRING) => statement = Some(reader.read_string()?),
            _ => reader.skip(field_type)?,
        }
    }
    let statement =
        statement.ok_or_else(|| HarborError::Thrift("ExecuteStatement missing SQL".into()))?;
    Ok(ExecuteStatementReq {
        session_id,
        statement,
    })
}

fn read_operation_req(reader: &mut Reader<'_>) -> Result<OperationReq> {
    let mut operation_id = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            operation_id = read_operation_handle(reader)?;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(OperationReq { operation_id })
}

fn read_fetch_results_req(reader: &mut Reader<'_>) -> Result<FetchResultsReq> {
    let mut operation_id = None;
    let mut start_row_offset = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => operation_id = read_operation_handle(reader)?,
            (1282, T_I64) => start_row_offset = Some(reader.read_i64()?),
            _ => reader.skip(field_type)?,
        }
    }
    Ok(FetchResultsReq {
        operation_id,
        start_row_offset,
    })
}

fn read_session_handle(reader: &mut Reader<'_>) -> Result<Option<String>> {
    let mut session_id = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            session_id = read_handle_identifier(reader)?;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(session_id)
}

fn read_operation_handle(reader: &mut Reader<'_>) -> Result<Option<String>> {
    let mut operation_id = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            operation_id = read_handle_identifier(reader)?;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(operation_id)
}

fn read_handle_identifier(reader: &mut Reader<'_>) -> Result<Option<String>> {
    let mut guid = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRING {
            let bytes = reader.read_binary()?;
            guid = Uuid::from_slice(&bytes).ok().map(|id| id.to_string());
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(guid)
}

fn write_open_session_response(
    seqid: i32,
    guid: &[u8; 16],
    secret: &[u8; 16],
    catalog: &str,
    schema: &str,
) -> Vec<u8> {
    write_success_response("OpenSession", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_field(T_I32, 2, |writer| {
            writer.write_i32(SPARK_CLI_SERVICE_PROTOCOL_V7)
        });
        writer.write_field(T_STRUCT, 3, |writer| {
            write_session_handle(writer, guid, secret);
        });
        writer.write_field(T_STRUCT, 1284, |writer| {
            write_namespace(writer, catalog, schema);
        });
        writer.write_field(T_BOOL, 1285, |writer| writer.write_bool(true));
        writer.write_stop();
    })
}

fn write_close_session_response(seqid: i32) -> Vec<u8> {
    write_success_response("CloseSession", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_stop();
    })
}

fn write_execute_statement_response(
    seqid: i32,
    guid: &[u8; 16],
    secret: &[u8; 16],
    result: &QueryResult,
) -> Vec<u8> {
    write_success_response("ExecuteStatement", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_field(T_STRUCT, 2, |writer| {
            write_operation_handle(writer, guid, secret, true);
        });
        writer.write_field(T_STRUCT, 1281, |writer| {
            write_direct_results(writer, result);
        });
        writer.write_stop();
    })
}

fn write_execute_statement_error(
    seqid: i32,
    guid: &[u8; 16],
    secret: &[u8; 16],
    error: &str,
) -> Vec<u8> {
    write_success_response("ExecuteStatement", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, ERROR_STATUS, Some(error))
        });
        writer.write_field(T_STRUCT, 2, |writer| {
            write_operation_handle(writer, guid, secret, false);
        });
        writer.write_stop();
    })
}

fn write_get_operation_status_response(
    seqid: i32,
    has_result_set: Option<bool>,
    valid: bool,
) -> Vec<u8> {
    write_success_response("GetOperationStatus", seqid, |writer| {
        write_operation_status_response(writer, has_result_set.unwrap_or(false), valid);
    })
}

fn write_get_result_set_metadata_response(seqid: i32, result: &QueryResult) -> Vec<u8> {
    write_success_response("GetResultSetMetadata", seqid, |writer| {
        write_result_set_metadata_response(writer, result, SUCCESS_STATUS);
    })
}

fn write_get_result_set_metadata_invalid(seqid: i32) -> Vec<u8> {
    write_success_response("GetResultSetMetadata", seqid, |writer| {
        write_result_set_metadata_response_with_error(writer, INVALID_HANDLE_STATUS);
    })
}

fn write_fetch_results_response(
    seqid: i32,
    result: &QueryResult,
    include_metadata: bool,
    start_row_offset: i64,
) -> Vec<u8> {
    write_success_response("FetchResults", seqid, |writer| {
        write_fetch_results_response_struct(writer, result, include_metadata, start_row_offset);
    })
}

fn write_fetch_results_invalid(seqid: i32) -> Vec<u8> {
    write_success_response("FetchResults", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(
                writer,
                INVALID_HANDLE_STATUS,
                Some("unknown operation handle"),
            );
        });
        writer.write_field(T_BOOL, 2, |writer| writer.write_bool(false));
        writer.write_stop();
    })
}

fn write_close_operation_response(seqid: i32) -> Vec<u8> {
    write_success_response("CloseOperation", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_stop();
    })
}

fn write_cancel_operation_response(seqid: i32) -> Vec<u8> {
    write_success_response("CancelOperation", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_stop();
    })
}

fn write_success_response<F>(method: &str, seqid: i32, write_success: F) -> Vec<u8>
where
    F: FnOnce(&mut Writer),
{
    let mut writer = Writer::new();
    writer.write_message_begin(method, T_MESSAGE_REPLY, seqid);
    writer.write_field(T_STRUCT, 0, |writer| {
        write_success(writer);
    });
    writer.write_stop();
    writer.into_inner()
}

fn write_application_exception(method: &str, seqid: i32, message: &str) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.write_message_begin(method, T_MESSAGE_EXCEPTION, seqid);
    writer.write_field(T_STRING, 1, |writer| writer.write_string(message));
    writer.write_field(T_I32, 2, |writer| writer.write_i32(0));
    writer.write_stop();
    writer.into_inner()
}

fn write_direct_results(writer: &mut Writer, result: &QueryResult) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_operation_status_response(writer, true, true);
    });
    writer.write_field(T_STRUCT, 2, |writer| {
        write_result_set_metadata_response(writer, result, SUCCESS_STATUS);
    });
    writer.write_field(T_STRUCT, 3, |writer| {
        write_fetch_results_response_struct(writer, result, false, 0);
    });
    writer.write_field(T_STRUCT, 4, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_stop();
    });
    writer.write_stop();
}

fn write_operation_status_response(writer: &mut Writer, has_result_set: bool, valid: bool) {
    if valid {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_field(T_I32, 2, |writer| writer.write_i32(FINISHED_STATE));
        writer.write_field(T_BOOL, 9, |writer| writer.write_bool(has_result_set));
    } else {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(
                writer,
                INVALID_HANDLE_STATUS,
                Some("unknown operation handle"),
            );
        });
        writer.write_field(T_I32, 2, |writer| writer.write_i32(FINISHED_STATE));
        writer.write_field(T_BOOL, 9, |writer| writer.write_bool(false));
    }
    writer.write_stop();
}

fn write_result_set_metadata_response(writer: &mut Writer, result: &QueryResult, status: i32) {
    writer.write_field(T_STRUCT, 1, |writer| write_status(writer, status, None));
    writer.write_field(T_STRUCT, 2, |writer| {
        write_table_schema(writer, &result.columns)
    });
    writer.write_field(T_I32, 1281, |writer| writer.write_i32(COLUMN_BASED_SET));
    writer.write_field(T_BOOL, 1282, |writer| writer.write_bool(false));
    writer.write_field(T_BOOL, 1287, |writer| writer.write_bool(false));
    writer.write_stop();
}

fn write_result_set_metadata_response_with_error(writer: &mut Writer, status: i32) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_status(writer, status, Some("unknown operation handle"));
    });
    writer.write_field(T_STRUCT, 2, |writer| write_table_schema(writer, &[]));
    writer.write_field(T_I32, 1281, |writer| writer.write_i32(COLUMN_BASED_SET));
    writer.write_field(T_BOOL, 1282, |writer| writer.write_bool(false));
    writer.write_stop();
}

fn write_fetch_results_response_struct(
    writer: &mut Writer,
    result: &QueryResult,
    include_metadata: bool,
    start_row_offset: i64,
) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_status(writer, SUCCESS_STATUS, None)
    });
    writer.write_field(T_BOOL, 2, |writer| writer.write_bool(false));
    writer.write_field(T_STRUCT, 3, |writer| {
        write_row_set(writer, result, start_row_offset);
    });
    if include_metadata {
        writer.write_field(T_STRUCT, 1281, |writer| {
            write_result_set_metadata_response(writer, result, SUCCESS_STATUS);
        });
    }
    writer.write_stop();
}

fn write_status(writer: &mut Writer, status_code: i32, message: Option<&str>) {
    writer.write_field(T_I32, 1, |writer| writer.write_i32(status_code));
    if let Some(message) = message {
        writer.write_field(T_STRING, 5, |writer| writer.write_string(message));
        writer.write_field(T_STRING, 6, |writer| writer.write_string(message));
    }
    writer.write_stop();
}

fn write_session_handle(writer: &mut Writer, guid: &[u8; 16], secret: &[u8; 16]) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_handle_identifier(writer, guid, secret);
    });
    writer.write_field(T_I32, 3329, |writer| {
        writer.write_i32(SPARK_CLI_SERVICE_PROTOCOL_V7)
    });
    writer.write_stop();
}

fn write_operation_handle(
    writer: &mut Writer,
    guid: &[u8; 16],
    secret: &[u8; 16],
    has_result_set: bool,
) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_handle_identifier(writer, guid, secret);
    });
    writer.write_field(T_I32, 2, |writer| writer.write_i32(EXECUTE_STATEMENT));
    writer.write_field(T_BOOL, 3, |writer| writer.write_bool(has_result_set));
    writer.write_stop();
}

fn write_handle_identifier(writer: &mut Writer, guid: &[u8; 16], secret: &[u8; 16]) {
    writer.write_field(T_STRING, 1, |writer| writer.write_binary(guid));
    writer.write_field(T_STRING, 2, |writer| writer.write_binary(secret));
    writer.write_stop();
}

fn write_namespace(writer: &mut Writer, catalog: &str, schema: &str) {
    writer.write_field(T_STRING, 1, |writer| writer.write_string(catalog));
    writer.write_field(T_STRING, 2, |writer| writer.write_string(schema));
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

fn write_row_set(writer: &mut Writer, result: &QueryResult, start_row_offset: i64) {
    let rows = result.rows.as_array().cloned().unwrap_or_default();
    writer.write_field(T_I64, 1, |writer| writer.write_i64(start_row_offset));
    writer.write_field(T_LIST, 3, |writer| {
        writer.write_list_begin(T_STRUCT, result.columns.len());
        for column in &result.columns {
            write_column(writer, column, &rows);
        }
    });
    writer.write_field(T_I32, 5, |writer| {
        writer.write_i32(result.columns.len() as i32)
    });
    writer.write_stop();
}

fn write_column(writer: &mut Writer, column: &Column, rows: &[Value]) {
    match column_kind(column).physical_type {
        PhysicalType::Bool => writer.write_field(T_STRUCT, 1, |writer| {
            write_column_values(writer, T_BOOL, rows, column, |writer, value| {
                writer.write_bool(value.as_bool().unwrap_or(false));
            });
        }),
        PhysicalType::I32 => writer.write_field(T_STRUCT, 4, |writer| {
            write_column_values(writer, T_I32, rows, column, |writer, value| {
                writer.write_i32(value_to_i64(value) as i32);
            });
        }),
        PhysicalType::I64 => writer.write_field(T_STRUCT, 5, |writer| {
            write_column_values(writer, T_I64, rows, column, |writer, value| {
                writer.write_i64(value_to_i64(value));
            });
        }),
        PhysicalType::F64 => writer.write_field(T_STRUCT, 6, |writer| {
            write_column_values(writer, T_DOUBLE, rows, column, |writer, value| {
                writer.write_double(value.as_f64().unwrap_or_default());
            });
        }),
        PhysicalType::String => writer.write_field(T_STRUCT, 7, |writer| {
            write_column_values(writer, T_STRING, rows, column, |writer, value| {
                writer.write_string(&value_to_string(value));
            });
        }),
    }
    writer.write_stop();
}

fn write_column_values<F>(
    writer: &mut Writer,
    value_type: u8,
    rows: &[Value],
    column: &Column,
    write_value: F,
) where
    F: Fn(&mut Writer, &Value),
{
    let nulls = null_bitset(rows, &column.name);
    writer.write_field(T_LIST, 1, |writer| {
        writer.write_list_begin(value_type, rows.len());
        for row in rows {
            let value = row
                .as_object()
                .and_then(|object| object.get(&column.name))
                .unwrap_or(&Value::Null);
            write_value(writer, value);
        }
    });
    writer.write_field(T_STRING, 2, |writer| writer.write_binary(&nulls));
    writer.write_stop();
}

fn null_bitset(rows: &[Value], column_name: &str) -> Vec<u8> {
    let mut nulls = vec![0_u8; rows.len().div_ceil(8)];
    for (index, row) in rows.iter().enumerate() {
        let is_null = row
            .as_object()
            .and_then(|object| object.get(column_name))
            .is_none_or(Value::is_null);
        if is_null {
            nulls[index >> 3] |= 1 << (index & 7);
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

fn value_to_i64(value: &Value) -> i64 {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_f64().map(|value| value as i64))
        .unwrap_or_default()
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn is_noop_statement(statement: &str) -> bool {
    let normalized = statement.trim().trim_end_matches(';').trim();
    let upper = normalized.to_ascii_uppercase();
    upper == "SET" || upper.starts_with("SET ")
}

struct Reader<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.position >= self.buffer.len() {
            return Err(HarborError::Thrift("unexpected end of message".into()));
        }
        let value = self.buffer[self.position];
        self.position += 1;
        Ok(value)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.position + len > self.buffer.len() {
            return Err(HarborError::Thrift("unexpected end of message".into()));
        }
        let bytes = &self.buffer[self.position..self.position + len];
        self.position += len;
        Ok(bytes)
    }

    fn read_i16(&mut self) -> Result<i16> {
        let bytes = self.read_exact(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.read_exact(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_double(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.read_i64()? as u64))
    }

    fn read_string(&mut self) -> Result<String> {
        let bytes = self.read_binary()?;
        String::from_utf8(bytes)
            .map_err(|err| HarborError::Thrift(format!("invalid UTF-8 string: {err}")))
    }

    fn read_binary(&mut self) -> Result<Vec<u8>> {
        let len = self.read_i32()?;
        if len < 0 {
            return Err(HarborError::Thrift("negative binary length".into()));
        }
        Ok(self.read_exact(len as usize)?.to_vec())
    }

    fn read_field_begin(&mut self) -> Result<(u8, i16)> {
        let field_type = self.read_u8()?;
        if field_type == T_STOP {
            return Ok((T_STOP, 0));
        }
        let field_id = self.read_i16()?;
        Ok((field_type, field_id))
    }

    fn skip_remaining_struct_fields(&mut self) -> Result<()> {
        loop {
            let (field_type, _) = self.read_field_begin()?;
            if field_type == T_STOP {
                return Ok(());
            }
            self.skip(field_type)?;
        }
    }

    fn skip(&mut self, field_type: u8) -> Result<()> {
        match field_type {
            T_STOP => Ok(()),
            T_BOOL | T_BYTE => {
                self.read_u8()?;
                Ok(())
            }
            T_I16 => {
                self.read_i16()?;
                Ok(())
            }
            T_I32 => {
                self.read_i32()?;
                Ok(())
            }
            T_I64 => {
                self.read_i64()?;
                Ok(())
            }
            T_DOUBLE => {
                self.read_double()?;
                Ok(())
            }
            T_STRING => {
                self.read_binary()?;
                Ok(())
            }
            T_STRUCT => self.skip_remaining_struct_fields(),
            T_MAP => {
                let key_type = self.read_u8()?;
                let value_type = self.read_u8()?;
                let size = self.read_i32()?;
                if size < 0 {
                    return Err(HarborError::Thrift("negative map size".into()));
                }
                for _ in 0..size {
                    self.skip(key_type)?;
                    self.skip(value_type)?;
                }
                Ok(())
            }
            T_LIST | T_SET => {
                let element_type = self.read_u8()?;
                let size = self.read_i32()?;
                if size < 0 {
                    return Err(HarborError::Thrift("negative list size".into()));
                }
                for _ in 0..size {
                    self.skip(element_type)?;
                }
                Ok(())
            }
            other => Err(HarborError::Thrift(format!(
                "unsupported Thrift type `{other}`"
            ))),
        }
    }
}

struct Writer {
    buffer: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    fn into_inner(self) -> Vec<u8> {
        self.buffer
    }

    fn write_message_begin(&mut self, name: &str, message_type: u8, seqid: i32) {
        self.write_i32((T_BINARY_VERSION_1 | message_type as u32) as i32);
        self.write_string(name);
        self.write_i32(seqid);
    }

    fn write_field<F>(&mut self, field_type: u8, field_id: i16, write_value: F)
    where
        F: FnOnce(&mut Writer),
    {
        self.buffer.push(field_type);
        self.write_i16(field_id);
        write_value(self);
    }

    fn write_stop(&mut self) {
        self.buffer.push(T_STOP);
    }

    fn write_list_begin(&mut self, element_type: u8, len: usize) {
        self.buffer.push(element_type);
        self.write_i32(len as i32);
    }

    fn write_bool(&mut self, value: bool) {
        self.buffer.push(u8::from(value));
    }

    fn write_i16(&mut self, value: i16) {
        self.buffer.extend(value.to_be_bytes());
    }

    fn write_i32(&mut self, value: i32) {
        self.buffer.extend(value.to_be_bytes());
    }

    fn write_i64(&mut self, value: i64) {
        self.buffer.extend(value.to_be_bytes());
    }

    fn write_double(&mut self, value: f64) {
        self.buffer.extend((value.to_bits() as i64).to_be_bytes());
    }

    fn write_string(&mut self, value: &str) {
        self.write_binary(value.as_bytes());
    }

    fn write_binary(&mut self, value: &[u8]) {
        self.write_i32(value.len() as i32);
        self.buffer.extend(value);
    }
}

#[allow(dead_code)]
fn _duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis() as u64
}
