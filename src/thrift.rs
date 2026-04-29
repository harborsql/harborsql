use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{sync::RwLock, task::JoinHandle};
use uuid::Uuid;

use crate::{
    config::Config,
    engine::{QueryEngine, QueryResult},
    error::{HarborError, Result},
};

mod codec;
mod protocol;
mod result_encoding;
mod state;

use codec::{Reader, Writer};
use protocol::*;
use result_encoding::{
    write_fetch_results_response_struct, write_result_set_metadata_response,
    write_result_set_metadata_response_with_error,
};
pub use state::QueryHistory;
use state::{
    Handle, OperationCompletion, OperationExecution, OperationState, SessionState,
    token_fingerprint,
};

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

    pub fn spawn_cleanup_task(&self) -> JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(service.config.cleanup_interval);
            loop {
                interval.tick().await;
                service.cleanup_expired().await;
            }
        })
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
                let request = read_args(message.payload, read_open_session_req)?;
                self.cleanup_expired().await;
                let session_id = Uuid::new_v4();
                let secret = Uuid::new_v4();
                let session_id_string = session_id.to_string();
                let catalog = request
                    .catalog
                    .unwrap_or_else(|| self.config.default_catalog.clone());
                let schema = request
                    .schema
                    .unwrap_or_else(|| self.config.default_schema.clone());

                let mut sessions = self.sessions.write().await;
                if sessions.len() >= self.config.max_sessions {
                    return Ok(write_open_session_error(
                        message.seqid,
                        "maximum session count exceeded",
                    ));
                }
                sessions.insert(
                    session_id_string.clone(),
                    SessionState {
                        id: session_id_string,
                        secret: *secret.as_bytes(),
                        token_fingerprint: token_fingerprint(bearer_token),
                        catalog: catalog.clone(),
                        schema: schema.clone(),
                        last_access: Instant::now(),
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
                let request = read_args(message.payload, read_close_session_req)?;
                let removed = self
                    .remove_session(request.session_handle.as_ref(), bearer_token)
                    .await;
                Ok(write_close_session_response(message.seqid, removed))
            }
            "ExecuteStatement" => {
                let request = read_args(message.payload, read_execute_statement_req)?;
                self.cleanup_expired().await;
                let Some(session) = self
                    .session_for(request.session_handle.as_ref(), bearer_token)
                    .await
                else {
                    return Ok(write_execute_statement_invalid(
                        message.seqid,
                        "invalid session handle",
                    ));
                };
                let operation_id = Uuid::new_v4();
                let secret = Uuid::new_v4();
                let query_id = operation_id.to_string();
                let statement = request.statement.trim().to_string();
                let engine = self.engine.clone();
                let token = bearer_token.to_string();
                let catalog = session.catalog.clone();
                let schema = session.schema.clone();
                let mut operations = self.operations.write().await;
                if operations.len() >= self.config.max_operations {
                    return Ok(write_execute_statement_error(
                        message.seqid,
                        "maximum operation count exceeded",
                    ));
                }
                let task = tokio::spawn(async move {
                    let started = Instant::now();
                    let result = if is_noop_statement(&statement) {
                        Ok(QueryResult::empty())
                    } else {
                        engine.execute(&token, &statement, &catalog, &schema).await
                    };
                    OperationCompletion {
                        duration_ms: started.elapsed().as_millis() as u64,
                        result: result.map_err(|err| {
                            err.log_internal("thrift operation");
                            err.client_error()
                        }),
                    }
                });

                operations.insert(
                    query_id,
                    OperationState {
                        secret: *secret.as_bytes(),
                        session_id: session.id.clone(),
                        token_fingerprint: token_fingerprint(bearer_token),
                        state: OperationExecution::Running {
                            started: Instant::now(),
                            task,
                        },
                    },
                );
                Ok(write_execute_statement_response(
                    message.seqid,
                    operation_id.as_bytes(),
                    secret.as_bytes(),
                ))
            }
            "GetOperationStatus" => {
                let request = read_args(message.payload, read_operation_req)?;
                Ok(self
                    .with_operation(
                        request.operation_handle.as_ref(),
                        bearer_token,
                        |operation| write_get_operation_status_response(message.seqid, operation),
                    )
                    .await
                    .unwrap_or_else(|| write_get_operation_status_invalid(message.seqid)))
            }
            "GetResultSetMetadata" => {
                let request = read_args(message.payload, read_operation_req)?;
                Ok(self
                    .with_operation(
                        request.operation_handle.as_ref(),
                        bearer_token,
                        |operation| match operation.state.result() {
                            Some(result) => {
                                write_get_result_set_metadata_response(message.seqid, result)
                            }
                            None => {
                                let error = operation.state.error_message();
                                write_get_result_set_metadata_not_ready(
                                    message.seqid,
                                    error.as_deref(),
                                )
                            }
                        },
                    )
                    .await
                    .unwrap_or_else(|| write_get_result_set_metadata_invalid(message.seqid)))
            }
            "FetchResults" => {
                let request = read_args(message.payload, read_fetch_results_req)?;
                Ok(self
                    .with_operation(
                        request.operation_handle.as_ref(),
                        bearer_token,
                        |operation| match operation.state.result() {
                            Some(result) => write_fetch_results_response(
                                message.seqid,
                                result,
                                true,
                                request.start_row_offset.unwrap_or(0),
                                request.max_rows,
                            ),
                            None => {
                                let error = operation.state.error_message();
                                write_fetch_results_not_ready(message.seqid, error.as_deref())
                            }
                        },
                    )
                    .await
                    .unwrap_or_else(|| write_fetch_results_invalid(message.seqid)))
            }
            "CloseOperation" => {
                let request = read_args(message.payload, read_operation_req)?;
                let removed = self
                    .remove_operation(request.operation_handle.as_ref(), bearer_token)
                    .await;
                Ok(write_close_operation_response(message.seqid, removed))
            }
            "CancelOperation" => {
                let request = read_args(message.payload, read_operation_req)?;
                let canceled = self
                    .cancel_operation(request.operation_handle.as_ref(), bearer_token)
                    .await;
                Ok(write_cancel_operation_response(message.seqid, canceled))
            }
            other => Ok(write_application_exception(
                other,
                message.seqid,
                &format!("unsupported Thrift method `{other}`"),
            )),
        }
    }
    pub async fn query_history(&self, bearer_token: &str, query_id: &str) -> Option<QueryHistory> {
        self.cleanup_expired().await;
        self.refresh_operation(query_id).await;
        let token_fingerprint = token_fingerprint(bearer_token);
        let operations = self.operations.read().await;
        let operation = operations
            .get(query_id)
            .filter(|operation| operation.token_fingerprint == token_fingerprint)?;
        let session_id = operation.session_id.clone();
        let history = QueryHistory {
            query_id: query_id.to_string(),
            status: operation.state.history_status().to_string(),
            duration: operation.state.duration_ms(),
        };
        drop(operations);
        self.touch_session(&session_id).await;
        Some(history)
    }

    async fn session_for(
        &self,
        handle: Option<&Handle>,
        bearer_token: &str,
    ) -> Option<SessionState> {
        let handle = handle?;
        let now = Instant::now();
        let token_fingerprint = token_fingerprint(bearer_token);
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(&handle.id)?;
        if session.is_expired(now, self.config.idle_session_timeout) {
            sessions.remove(&handle.id);
            drop(sessions);
            self.remove_operations_for_session(&handle.id).await;
            return None;
        }
        if session.secret == handle.secret && session.token_fingerprint == token_fingerprint {
            session.last_access = now;
            Some(session.clone())
        } else {
            None
        }
    }

    async fn remove_session(&self, handle: Option<&Handle>, bearer_token: &str) -> bool {
        let Some(handle) = handle else {
            return false;
        };
        let token_fingerprint = token_fingerprint(bearer_token);
        let mut sessions = self.sessions.write().await;
        let valid = sessions.get(&handle.id).is_some_and(|session| {
            session.secret == handle.secret && session.token_fingerprint == token_fingerprint
        });
        if !valid {
            return false;
        }
        sessions.remove(&handle.id);
        drop(sessions);

        self.remove_operations_for_session(&handle.id).await;
        true
    }

    async fn remove_operations_for_session(&self, session_id: &str) {
        self.operations.write().await.retain(|_, operation| {
            if operation.session_id == session_id {
                operation.abort();
                false
            } else {
                true
            }
        });
    }

    async fn touch_session(&self, session_id: &str) {
        if let Some(session) = self.sessions.write().await.get_mut(session_id) {
            session.last_access = Instant::now();
        }
    }

    async fn with_operation<R>(
        &self,
        handle: Option<&Handle>,
        bearer_token: &str,
        write: impl FnOnce(&OperationState) -> R,
    ) -> Option<R> {
        let handle = handle?;
        let token_fingerprint = token_fingerprint(bearer_token);
        self.cleanup_expired().await;
        self.refresh_operation(&handle.id).await;
        let operations = self.operations.read().await;
        let operation = operations.get(&handle.id)?;
        if operation.secret == handle.secret && operation.token_fingerprint == token_fingerprint {
            let session_id = operation.session_id.clone();
            let response = write(operation);
            drop(operations);
            self.touch_session(&session_id).await;
            Some(response)
        } else {
            None
        }
    }

    async fn remove_operation(&self, handle: Option<&Handle>, bearer_token: &str) -> bool {
        let Some(handle) = handle else {
            return false;
        };
        let token_fingerprint = token_fingerprint(bearer_token);
        self.cleanup_expired().await;
        let mut operations = self.operations.write().await;
        let valid = operations.get(&handle.id).is_some_and(|operation| {
            operation.secret == handle.secret && operation.token_fingerprint == token_fingerprint
        });
        if valid && let Some(operation) = operations.remove(&handle.id) {
            operation.abort();
        }
        valid
    }

    async fn cancel_operation(&self, handle: Option<&Handle>, bearer_token: &str) -> bool {
        let Some(handle) = handle else {
            return false;
        };
        let token_fingerprint = token_fingerprint(bearer_token);
        self.cleanup_expired().await;
        let mut operations = self.operations.write().await;
        let Some(operation) = operations.get_mut(&handle.id) else {
            return false;
        };
        if operation.secret != handle.secret || operation.token_fingerprint != token_fingerprint {
            return false;
        }
        operation.cancel();
        true
    }

    async fn refresh_operation(&self, operation_id: &str) {
        let finished_task = {
            let mut operations = self.operations.write().await;
            operations
                .get_mut(operation_id)
                .and_then(OperationState::take_finished_task)
        };
        if let Some((started, task)) = finished_task {
            let result = task.await;
            let mut operations = self.operations.write().await;
            if let Some(operation) = operations.get_mut(operation_id) {
                operation.finish_refresh(started, result);
            }
        };
    }

    async fn refresh_finished_operations(&self) {
        let operation_ids = {
            let operations = self.operations.read().await;
            operations
                .iter()
                .filter(|(_, operation)| operation.has_finished_task())
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        };
        for operation_id in operation_ids {
            self.refresh_operation(&operation_id).await;
        }
    }

    async fn cleanup_expired(&self) {
        self.refresh_finished_operations().await;

        let now = Instant::now();
        let expired_sessions = {
            let mut sessions = self.sessions.write().await;
            let expired_sessions = sessions
                .iter()
                .filter(|(_, session)| session.is_expired(now, self.config.idle_session_timeout))
                .map(|(id, _)| id.clone())
                .collect::<HashSet<_>>();
            for session_id in &expired_sessions {
                sessions.remove(session_id);
            }
            expired_sessions
        };

        self.operations.write().await.retain(|_, operation| {
            if expired_sessions.contains(&operation.session_id)
                || operation.is_expired(now, self.config.completed_operation_ttl)
            {
                operation.abort();
                false
            } else {
                true
            }
        });
    }
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
    session_handle: Option<Handle>,
}

#[derive(Debug)]
struct ExecuteStatementReq {
    session_handle: Option<Handle>,
    statement: String,
}

#[derive(Debug)]
struct OperationReq {
    operation_handle: Option<Handle>,
}

#[derive(Debug)]
struct FetchResultsReq {
    operation_handle: Option<Handle>,
    start_row_offset: Option<i64>,
    max_rows: Option<i64>,
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
    let mut session_handle = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            session_handle = read_session_handle(reader)?;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(CloseSessionReq { session_handle })
}

fn read_execute_statement_req(reader: &mut Reader<'_>) -> Result<ExecuteStatementReq> {
    let mut session_handle = None;
    let mut statement = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => session_handle = read_session_handle(reader)?,
            (2, T_STRING) => statement = Some(reader.read_string()?),
            _ => reader.skip(field_type)?,
        }
    }
    let statement =
        statement.ok_or_else(|| HarborError::Thrift("ExecuteStatement missing SQL".into()))?;
    Ok(ExecuteStatementReq {
        session_handle,
        statement,
    })
}

fn read_operation_req(reader: &mut Reader<'_>) -> Result<OperationReq> {
    let mut operation_handle = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            operation_handle = read_operation_handle(reader)?;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(OperationReq { operation_handle })
}

fn read_fetch_results_req(reader: &mut Reader<'_>) -> Result<FetchResultsReq> {
    let mut operation_handle = None;
    let mut start_row_offset = None;
    let mut max_rows = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => operation_handle = read_operation_handle(reader)?,
            (3, T_I64) => max_rows = Some(reader.read_i64()?),
            (1282, T_I64) => start_row_offset = Some(reader.read_i64()?),
            _ => reader.skip(field_type)?,
        }
    }
    Ok(FetchResultsReq {
        operation_handle,
        start_row_offset,
        max_rows,
    })
}

fn read_session_handle(reader: &mut Reader<'_>) -> Result<Option<Handle>> {
    let mut session_handle = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            session_handle = read_handle_identifier(reader)?;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(session_handle)
}

fn read_operation_handle(reader: &mut Reader<'_>) -> Result<Option<Handle>> {
    let mut operation_handle = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        if field_id == 1 && field_type == T_STRUCT {
            operation_handle = read_handle_identifier(reader)?;
        } else {
            reader.skip(field_type)?;
        }
    }
    Ok(operation_handle)
}

fn read_handle_identifier(reader: &mut Reader<'_>) -> Result<Option<Handle>> {
    let mut guid = None;
    let mut secret = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRING) => {
                let bytes = reader.read_binary()?;
                guid = Uuid::from_slice(&bytes).ok().map(|id| id.to_string());
            }
            (2, T_STRING) => {
                let bytes = reader.read_binary()?;
                secret = bytes.as_slice().try_into().ok();
            }
            _ => reader.skip(field_type)?,
        }
    }
    Ok(match (guid, secret) {
        (Some(id), Some(secret)) => Some(Handle { id, secret }),
        _ => None,
    })
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

fn write_open_session_error(seqid: i32, message: &str) -> Vec<u8> {
    write_success_response("OpenSession", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, ERROR_STATUS, Some(message))
        });
        writer.write_stop();
    })
}

fn write_close_session_response(seqid: i32, valid: bool) -> Vec<u8> {
    write_success_response("CloseSession", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(
                writer,
                if valid {
                    SUCCESS_STATUS
                } else {
                    INVALID_HANDLE_STATUS
                },
                if valid {
                    None
                } else {
                    Some("invalid session handle")
                },
            )
        });
        writer.write_stop();
    })
}

fn write_execute_statement_response(seqid: i32, guid: &[u8; 16], secret: &[u8; 16]) -> Vec<u8> {
    write_success_response("ExecuteStatement", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_field(T_STRUCT, 2, |writer| {
            write_operation_handle(writer, guid, secret, true);
        });
        writer.write_stop();
    })
}

fn write_execute_statement_error(seqid: i32, message: &str) -> Vec<u8> {
    write_success_response("ExecuteStatement", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, ERROR_STATUS, Some(message))
        });
        writer.write_stop();
    })
}

fn write_execute_statement_invalid(seqid: i32, message: &str) -> Vec<u8> {
    write_success_response("ExecuteStatement", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, INVALID_HANDLE_STATUS, Some(message))
        });
        writer.write_stop();
    })
}

fn write_get_operation_status_response(seqid: i32, operation: &OperationState) -> Vec<u8> {
    write_success_response("GetOperationStatus", seqid, |writer| {
        let error = operation.state.error_message();
        write_operation_status_response(
            writer,
            operation.state.status_code(),
            operation.state.operation_state(),
            operation.state.result().is_some(),
            error.as_deref(),
        );
    })
}

fn write_get_operation_status_invalid(seqid: i32) -> Vec<u8> {
    write_success_response("GetOperationStatus", seqid, |writer| {
        write_operation_status_response(
            writer,
            INVALID_HANDLE_STATUS,
            ERROR_STATE,
            false,
            Some("unknown operation handle"),
        );
    })
}

fn write_get_result_set_metadata_response(seqid: i32, result: &QueryResult) -> Vec<u8> {
    write_success_response("GetResultSetMetadata", seqid, |writer| {
        write_result_set_metadata_response(writer, result, SUCCESS_STATUS);
    })
}

fn write_get_result_set_metadata_invalid(seqid: i32) -> Vec<u8> {
    write_success_response("GetResultSetMetadata", seqid, |writer| {
        write_result_set_metadata_response_with_error(
            writer,
            INVALID_HANDLE_STATUS,
            "unknown operation handle",
        );
    })
}

fn write_get_result_set_metadata_not_ready(seqid: i32, message: Option<&str>) -> Vec<u8> {
    write_success_response("GetResultSetMetadata", seqid, |writer| {
        write_result_set_metadata_response_with_error(
            writer,
            ERROR_STATUS,
            message.unwrap_or("operation is not finished"),
        );
    })
}

fn write_fetch_results_response(
    seqid: i32,
    result: &QueryResult,
    include_metadata: bool,
    start_row_offset: i64,
    max_rows: Option<i64>,
) -> Vec<u8> {
    write_success_response("FetchResults", seqid, |writer| {
        write_fetch_results_response_struct(
            writer,
            result,
            include_metadata,
            start_row_offset,
            max_rows,
        );
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

fn write_fetch_results_not_ready(seqid: i32, message: Option<&str>) -> Vec<u8> {
    write_success_response("FetchResults", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(
                writer,
                ERROR_STATUS,
                Some(message.unwrap_or("operation is not finished")),
            );
        });
        writer.write_field(T_BOOL, 2, |writer| writer.write_bool(false));
        writer.write_stop();
    })
}

fn write_close_operation_response(seqid: i32, valid: bool) -> Vec<u8> {
    write_success_response("CloseOperation", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(
                writer,
                if valid {
                    SUCCESS_STATUS
                } else {
                    INVALID_HANDLE_STATUS
                },
                if valid {
                    None
                } else {
                    Some("unknown operation handle")
                },
            )
        });
        writer.write_stop();
    })
}

fn write_cancel_operation_response(seqid: i32, valid: bool) -> Vec<u8> {
    write_success_response("CancelOperation", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(
                writer,
                if valid {
                    SUCCESS_STATUS
                } else {
                    INVALID_HANDLE_STATUS
                },
                if valid {
                    None
                } else {
                    Some("unknown operation handle")
                },
            )
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

fn write_operation_status_response(
    writer: &mut Writer,
    status_code: i32,
    operation_state: i32,
    has_result_set: bool,
    message: Option<&str>,
) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_status(writer, status_code, message)
    });
    writer.write_field(T_I32, 2, |writer| writer.write_i32(operation_state));
    writer.write_field(T_BOOL, 9, |writer| writer.write_bool(has_result_set));
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

fn is_noop_statement(statement: &str) -> bool {
    let normalized = statement.trim().trim_end_matches(';').trim();
    let upper = normalized.to_ascii_uppercase();
    upper == "SET" || upper.starts_with("SET ")
}

#[allow(dead_code)]
fn _duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis() as u64
}

#[cfg(test)]
mod tests;
