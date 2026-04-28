use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::{
    sync::RwLock,
    task::{JoinError, JoinHandle},
};
use uuid::Uuid;

use crate::{
    config::Config,
    engine::{Column, QueryEngine, QueryResult, QueryResultPage},
    error::{ClientError, HarborError, Result, redact_sensitive},
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
const RUNNING_STATE: i32 = 1;
const FINISHED_STATE: i32 = 2;
const CANCELED_STATE: i32 = 3;
const ERROR_STATE: i32 = 5;
const EXECUTE_STATEMENT: i32 = 0;
const COLUMN_BASED_SET: i32 = 1;
const DEFAULT_FETCH_ROWS: usize = 1_000;
const MAX_FETCH_ROWS: usize = 10_000;

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

#[derive(Debug, Serialize)]
pub struct QueryHistory {
    pub query_id: String,
    pub status: String,
    pub duration: u64,
}

#[derive(Debug, Clone)]
struct SessionState {
    id: String,
    secret: [u8; 16],
    token_fingerprint: [u8; 32],
    catalog: String,
    schema: String,
    last_access: Instant,
}

impl SessionState {
    fn is_expired(&self, now: Instant, idle_timeout: Duration) -> bool {
        now.saturating_duration_since(self.last_access) >= idle_timeout
    }
}

struct OperationState {
    secret: [u8; 16],
    session_id: String,
    token_fingerprint: [u8; 32],
    state: OperationExecution,
}

impl OperationState {
    fn has_finished_task(&self) -> bool {
        matches!(&self.state, OperationExecution::Running { task, .. } if task.is_finished())
    }

    fn take_finished_task(&mut self) -> Option<(Instant, JoinHandle<OperationCompletion>)> {
        if !self.has_finished_task() {
            return None;
        }

        let state = std::mem::replace(
            &mut self.state,
            OperationExecution::Refreshing {
                started: Instant::now(),
            },
        );
        let OperationExecution::Running { started, task } = state else {
            self.state = state;
            return None;
        };
        self.state = OperationExecution::Refreshing { started };
        Some((started, task))
    }

    fn finish_refresh(
        &mut self,
        started: Instant,
        result: std::result::Result<OperationCompletion, JoinError>,
    ) {
        if matches!(self.state, OperationExecution::Refreshing { .. }) {
            self.state = OperationExecution::from_join_result(started, result);
        }
    }

    fn abort(&self) {
        if let OperationExecution::Running { task, .. } = &self.state {
            task.abort();
        }
    }

    fn cancel(&mut self) {
        if !matches!(self.state, OperationExecution::Running { .. }) {
            return;
        }
        let duration_ms = self.state.duration_ms();
        self.abort();
        self.state = OperationExecution::Canceled {
            duration_ms,
            completed_at: Instant::now(),
        };
    }

    fn is_expired(&self, now: Instant, completed_operation_ttl: Duration) -> bool {
        self.state.is_expired(now, completed_operation_ttl)
    }
}

enum OperationExecution {
    Running {
        started: Instant,
        task: JoinHandle<OperationCompletion>,
    },
    Refreshing {
        started: Instant,
    },
    Finished {
        result: Arc<QueryResult>,
        duration_ms: u64,
        completed_at: Instant,
    },
    Failed {
        error: ClientError,
        duration_ms: u64,
        completed_at: Instant,
    },
    Canceled {
        duration_ms: u64,
        completed_at: Instant,
    },
}

impl OperationExecution {
    fn from_completion(completion: OperationCompletion) -> Self {
        let completed_at = Instant::now();
        match completion.result {
            Ok(result) => Self::Finished {
                result: Arc::new(result),
                duration_ms: completion.duration_ms,
                completed_at,
            },
            Err(error) => Self::Failed {
                error,
                duration_ms: completion.duration_ms,
                completed_at,
            },
        }
    }

    fn from_join_result(
        started: Instant,
        result: std::result::Result<OperationCompletion, JoinError>,
    ) -> Self {
        match result {
            Ok(completion) => Self::from_completion(completion),
            Err(err) if err.is_cancelled() => Self::Canceled {
                duration_ms: started.elapsed().as_millis() as u64,
                completed_at: Instant::now(),
            },
            Err(err) => {
                let error = ClientError::internal();
                tracing::error!(
                    error_code = error.code,
                    internal_error = %redact_sensitive(&err.to_string()),
                    "operation task failed"
                );
                Self::Failed {
                    error,
                    duration_ms: started.elapsed().as_millis() as u64,
                    completed_at: Instant::now(),
                }
            }
        }
    }

    fn result(&self) -> Option<&QueryResult> {
        match self {
            Self::Finished { result, .. } => Some(result.as_ref()),
            _ => None,
        }
    }

    fn status_code(&self) -> i32 {
        match self {
            Self::Failed { .. } => ERROR_STATUS,
            _ => SUCCESS_STATUS,
        }
    }

    fn operation_state(&self) -> i32 {
        match self {
            Self::Running { .. } | Self::Refreshing { .. } => RUNNING_STATE,
            Self::Finished { .. } => FINISHED_STATE,
            Self::Failed { .. } => ERROR_STATE,
            Self::Canceled { .. } => CANCELED_STATE,
        }
    }

    fn duration_ms(&self) -> u64 {
        match self {
            Self::Running { started, .. } | Self::Refreshing { started } => {
                started.elapsed().as_millis() as u64
            }
            Self::Finished { duration_ms, .. }
            | Self::Failed { duration_ms, .. }
            | Self::Canceled { duration_ms, .. } => *duration_ms,
        }
    }

    fn history_status(&self) -> &'static str {
        match self {
            Self::Running { .. } | Self::Refreshing { .. } => "RUNNING",
            Self::Finished { .. } => "FINISHED",
            Self::Failed { .. } => "FAILED",
            Self::Canceled { .. } => "CANCELED",
        }
    }

    fn error_message(&self) -> Option<String> {
        match self {
            Self::Running { .. } | Self::Refreshing { .. } => {
                Some("OPERATION_RUNNING: operation is still running".to_string())
            }
            Self::Failed { error, .. } => Some(error.status_message()),
            Self::Canceled { .. } => Some("OPERATION_CANCELED: operation was canceled".to_string()),
            Self::Finished { .. } => None,
        }
    }

    fn is_expired(&self, now: Instant, completed_operation_ttl: Duration) -> bool {
        let completed_at = match self {
            Self::Finished { completed_at, .. }
            | Self::Failed { completed_at, .. }
            | Self::Canceled { completed_at, .. } => completed_at,
            Self::Running { .. } | Self::Refreshing { .. } => return false,
        };
        now.saturating_duration_since(*completed_at) >= completed_operation_ttl
    }
}

struct OperationCompletion {
    duration_ms: u64,
    result: std::result::Result<QueryResult, ClientError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Handle {
    id: String,
    secret: [u8; 16],
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

fn write_result_set_metadata_response_with_error(writer: &mut Writer, status: i32, message: &str) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_status(writer, status, Some(message));
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

fn write_row_set(writer: &mut Writer, result: &QueryResult, page: &QueryResultPage) {
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

fn row_page(result: &QueryResult, start_row_offset: i64, max_rows: Option<i64>) -> QueryResultPage {
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

fn null_bitset(batches: &[RecordBatch], column_index: usize, row_count: usize) -> Vec<u8> {
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

fn is_noop_statement(statement: &str) -> bool {
    let normalized = statement.trim().trim_end_matches(';').trim();
    let upper = normalized.to_ascii_uppercase();
    upper == "SET" || upper.starts_with("SET ")
}

fn token_fingerprint(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::{
        array::{Array, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;

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
}
