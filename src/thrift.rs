use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{
    sync::{RwLock, oneshot},
    task::JoinHandle,
};
use tracing::{Instrument, field};
use uuid::Uuid;

use crate::{
    config::Config,
    engine::{GetColumnsMetadataRequest, QueryEngine, QueryResult},
    error::{ClientError, HarborError, Result},
    observability,
};

mod codec;
mod protocol;
mod result_encoding;
mod state;

use codec::{Reader, Writer};
use protocol::*;
use result_encoding::{
    row_page, write_fetch_results_response_struct, write_result_set_metadata_response,
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

enum OperationFinishOutcome {
    Succeeded,
    Failed(String),
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
        let started = Instant::now();
        let message = match decode_message(body) {
            Ok(message) => message,
            Err(error) => {
                observability::get()
                    .metrics()
                    .observe_thrift("decode", "error", started.elapsed());
                return Err(error);
            }
        };
        let method = message.name.clone();
        let seqid = message.seqid;
        let result = self
            .handle_decoded(bearer_token, message)
            .instrument(tracing::info_span!("thrift_rpc", method = %method, seqid))
            .await;
        observability::get().metrics().observe_thrift(
            &method,
            if result.is_ok() { "ok" } else { "error" },
            started.elapsed(),
        );
        if let Ok(response) = &result {
            observability::get().metrics().add(
                "harborsql_thrift_response_bytes_total",
                response.len() as u64,
            );
        }
        result
    }

    async fn handle_decoded(&self, bearer_token: &str, message: Message<'_>) -> Result<Vec<u8>> {
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
                        id: session_id_string.clone(),
                        secret: *secret.as_bytes(),
                        token_fingerprint: token_fingerprint(bearer_token),
                        catalog: catalog.clone(),
                        schema: schema.clone(),
                        last_access: Instant::now(),
                    },
                );
                record_session_gauge(&sessions);
                tracing::info!(
                    session_id = %session_id_string,
                    catalog_hash = %observability::stable_hash(&catalog),
                    schema_hash = %observability::stable_hash(&schema),
                    "Thrift session opened"
                );

                Ok(write_open_session_response(
                    message.seqid,
                    session_id.as_bytes(),
                    secret.as_bytes(),
                    &catalog,
                    &schema,
                    &request.get_info_types,
                ))
            }
            "CloseSession" => {
                let request = read_args(message.payload, read_close_session_req)?;
                let removed = self
                    .remove_session(request.session_handle.as_ref(), bearer_token)
                    .await;
                Ok(write_close_session_response(message.seqid, removed))
            }
            "GetInfo" => {
                let request = read_args(message.payload, read_get_info_req)?;
                let Some(session) = self
                    .session_for(request.session_handle.as_ref(), bearer_token)
                    .await
                else {
                    return Ok(write_get_info_invalid(message.seqid));
                };
                Ok(write_get_info_response(
                    message.seqid,
                    request.info_type,
                    &session.catalog,
                ))
            }
            "GetColumns" => {
                let request = read_args(message.payload, read_get_columns_req)?;
                self.cleanup_expired().await;
                let Some(session) = self
                    .session_for(request.session_handle.as_ref(), bearer_token)
                    .await
                else {
                    return Ok(write_get_columns_invalid(
                        message.seqid,
                        "invalid session handle",
                    ));
                };
                let operation_id = Uuid::new_v4();
                let secret = Uuid::new_v4();
                let query_id = operation_id.to_string();
                let metadata_catalog = request.catalog_name;
                let metadata_schema = request.schema_name;
                let metadata_table = request.table_name;
                let metadata_column = request.column_name;
                let engine = self.engine.clone();
                let token = bearer_token.to_string();
                let default_catalog = session.catalog.clone();
                let default_schema = session.schema.clone();
                let session_id = session.id.clone();
                let execution_query_id = query_id.clone();
                let completion_query_id = query_id.clone();
                let operations_for_task = self.operations.clone();
                let (completion_sender, completion_receiver) = oneshot::channel();
                let operation_started = Instant::now();
                let operation_span = tracing::info_span!(
                    "thrift_metadata_operation",
                    method = "GetColumns",
                    query_id = %query_id,
                    session_id = %session.id
                );
                let mut operations = self.operations.write().await;
                if operations.len() >= self.config.max_operations {
                    return Ok(write_get_columns_error(
                        message.seqid,
                        "maximum operation count exceeded",
                    ));
                }

                let task = tokio::spawn(async move {
                    let completion = async move {
                        let result = engine
                            .get_columns_metadata(
                                &token,
                                GetColumnsMetadataRequest {
                                    catalog: metadata_catalog.as_deref(),
                                    schema: metadata_schema.as_deref(),
                                    table: metadata_table.as_deref(),
                                    column: metadata_column.as_deref(),
                                },
                                &default_catalog,
                                &default_schema,
                            )
                            .await;
                        let duration = operation_started.elapsed();
                        match &result {
                            Ok(result) => {
                                tracing::info!(
                                    query_id = %execution_query_id,
                                    session_id = %session_id,
                                    duration_ms = duration.as_millis() as u64,
                                    row_count = result.row_count,
                                    "Thrift metadata operation finished"
                                );
                            }
                            Err(error) => error.log_internal("thrift GetColumns metadata"),
                        }
                        OperationCompletion {
                            duration_ms: duration.as_millis() as u64,
                            result: result.map_err(|err| err.client_error()),
                        }
                    }
                    .instrument(operation_span)
                    .await;
                    let outcome = operation_completion_outcome(&completion);
                    let mut operations = operations_for_task.write().await;
                    let operation_completed = if let Some(operation) =
                        operations.get_mut(&completion_query_id)
                    {
                        operation.state = OperationExecution::from_completion(completion.clone());
                        tracing::info!(
                            query_id = %completion_query_id,
                            status = operation.state.history_status(),
                            duration_ms = operation.state.duration_ms(),
                            "Thrift operation completed"
                        );
                        true
                    } else {
                        false
                    };
                    let outcome_delivered = completion_sender.send(outcome).is_ok();
                    if operation_completed
                        && !outcome_delivered
                        && operations.remove(&completion_query_id).is_some()
                    {
                        tracing::info!(
                            query_id = %completion_query_id,
                            "Thrift metadata operation removed before handle returned"
                        );
                    }
                    record_operation_gauges(&operations);
                    completion
                });

                operations.insert(
                    query_id.clone(),
                    OperationState {
                        secret: *secret.as_bytes(),
                        session_id: session.id.clone(),
                        token_fingerprint: token_fingerprint(bearer_token),
                        state: OperationExecution::Running {
                            started: operation_started,
                            task,
                        },
                    },
                );
                record_operation_gauges(&operations);
                drop(operations);

                match await_operation_completion(completion_receiver).await {
                    OperationFinishOutcome::Succeeded => Ok(write_get_columns_response(
                        message.seqid,
                        operation_id.as_bytes(),
                        secret.as_bytes(),
                    )),
                    OperationFinishOutcome::Failed(error_message) => {
                        self.remove_unreturned_operation(&query_id).await;
                        Ok(write_get_columns_error(message.seqid, &error_message))
                    }
                }
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
                let execution_query_id = query_id.clone();
                let mut operations = self.operations.write().await;
                if operations.len() >= self.config.max_operations {
                    return Ok(write_execute_statement_error(
                        message.seqid,
                        "maximum operation count exceeded",
                    ));
                }
                let sql_observation = observability::get().sql_observation(&statement);
                let operation_span = tracing::info_span!(
                    "thrift_operation",
                    query_id = %query_id,
                    session_id = %session.id,
                    sql_hash = %sql_observation.hash,
                    sql_len = sql_observation.len,
                    sql = field::Empty,
                );
                if let Some(sql) = sql_observation.text.as_deref() {
                    operation_span.record("sql", field::display(sql));
                }
                observability::get()
                    .metrics()
                    .increment("harborsql_thrift_operations_started_total");
                tracing::info!(
                    query_id = %query_id,
                    session_id = %session.id,
                    sql_hash = %sql_observation.hash,
                    "Thrift operation started"
                );
                let task = tokio::spawn(async move {
                    async move {
                        let started = Instant::now();
                        let result = if is_noop_statement(&statement) {
                            Ok(QueryResult::empty())
                        } else {
                            engine
                                .execute_with_query_id(
                                    Some(&execution_query_id),
                                    &token,
                                    &statement,
                                    &catalog,
                                    &schema,
                                )
                                .await
                        };
                        let duration = started.elapsed();
                        observability::get()
                            .metrics()
                            .observe_duration("thrift_operation_execution", duration);
                        match &result {
                            Ok(result) => {
                                observability::get()
                                    .metrics()
                                    .increment("harborsql_thrift_operations_succeeded_total");
                                tracing::info!(
                                    duration_ms = duration.as_millis() as u64,
                                    row_count = result.row_count,
                                    "Thrift operation execution finished"
                                );
                            }
                            Err(error) => {
                                observability::get()
                                    .metrics()
                                    .increment("harborsql_thrift_operations_failed_total");
                                error.log_internal("thrift operation");
                            }
                        }
                        OperationCompletion {
                            duration_ms: duration.as_millis() as u64,
                            result: result.map_err(|err| err.client_error()),
                        }
                    }
                    .instrument(operation_span)
                    .await
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
                record_operation_gauges(&operations);
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
            "GetQueryId" => {
                let request = read_args(message.payload, read_operation_req)?;
                let query_id = request
                    .operation_handle
                    .as_ref()
                    .map(|handle| handle.id.as_str())
                    .unwrap_or("");
                Ok(self
                    .with_operation(request.operation_handle.as_ref(), bearer_token, |_| {
                        write_get_query_id_response(message.seqid, query_id)
                    })
                    .await
                    .unwrap_or_else(|| write_get_query_id_response(message.seqid, "")))
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
                let query_id = request
                    .operation_handle
                    .as_ref()
                    .map(|handle| handle.id.as_str())
                    .unwrap_or("");
                Ok(self
                    .with_operation(
                        request.operation_handle.as_ref(),
                        bearer_token,
                        |operation| match operation.state.result() {
                            Some(result) => {
                                let start_row_offset = request.start_row_offset.unwrap_or(0);
                                let page = row_page(result, start_row_offset, request.max_rows);
                                let response = write_fetch_results_response(
                                    message.seqid,
                                    result,
                                    true,
                                    start_row_offset,
                                    request.max_rows,
                                );
                                let metrics = observability::get().metrics();
                                metrics.increment("harborsql_thrift_fetches_total");
                                metrics.add(
                                    "harborsql_thrift_fetch_rows_total",
                                    page.row_count as u64,
                                );
                                metrics.add(
                                    "harborsql_thrift_fetch_response_bytes_total",
                                    response.len() as u64,
                                );
                                tracing::info!(
                                    row_count = page.row_count,
                                    has_more_rows = page.has_more_rows,
                                    response_bytes = response.len(),
                                    "Thrift results fetched"
                                );
                                response
                            }
                            None => {
                                let error = operation.state.error_message();
                                write_fetch_results_not_ready(message.seqid, error.as_deref())
                            }
                        },
                    )
                    .instrument(tracing::info_span!("thrift_fetch", query_id = %query_id))
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
                let query_id = request
                    .operation_handle
                    .as_ref()
                    .map(|handle| handle.id.as_str())
                    .unwrap_or("");
                let canceled = self
                    .cancel_operation(request.operation_handle.as_ref(), bearer_token)
                    .instrument(tracing::info_span!("thrift_cancel", query_id = %query_id))
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
        record_session_gauge(&sessions);
        drop(sessions);

        self.remove_operations_for_session(&handle.id).await;
        tracing::info!(session_id = %handle.id, "Thrift session closed");
        true
    }

    async fn remove_operations_for_session(&self, session_id: &str) {
        let mut operations = self.operations.write().await;
        operations.retain(|_, operation| {
            if operation.session_id == session_id {
                operation.abort();
                false
            } else {
                true
            }
        });
        record_operation_gauges(&operations);
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
            observability::get()
                .metrics()
                .increment("harborsql_thrift_operations_closed_total");
            record_operation_gauges(&operations);
            tracing::info!(query_id = %handle.id, "Thrift operation closed");
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
        observability::get()
            .metrics()
            .increment("harborsql_thrift_operations_canceled_total");
        record_operation_gauges(&operations);
        tracing::info!(query_id = %handle.id, "Thrift operation canceled");
        true
    }

    async fn remove_unreturned_operation(&self, operation_id: &str) {
        let mut operations = self.operations.write().await;
        if let Some(operation) = operations.remove(operation_id) {
            operation.abort();
            record_operation_gauges(&operations);
        }
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
                tracing::info!(
                    query_id = %operation_id,
                    status = operation.state.history_status(),
                    duration_ms = operation.state.duration_ms(),
                    "Thrift operation completed"
                );
            }
            record_operation_gauges(&operations);
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

        let mut operations = self.operations.write().await;
        operations.retain(|_, operation| {
            if expired_sessions.contains(&operation.session_id)
                || operation.is_expired(now, self.config.completed_operation_ttl)
            {
                operation.abort();
                false
            } else {
                true
            }
        });
        record_operation_gauges(&operations);
    }
}

fn record_session_gauge(sessions: &HashMap<String, SessionState>) {
    observability::get()
        .metrics()
        .set_gauge("harborsql_thrift_sessions", sessions.len() as i64);
}

fn record_operation_gauges(operations: &HashMap<String, OperationState>) {
    let active = operations
        .values()
        .filter(|operation| operation.is_active())
        .count();
    let metrics = observability::get().metrics();
    metrics.set_gauge("harborsql_thrift_operations", operations.len() as i64);
    metrics.set_gauge("harborsql_thrift_active_operations", active as i64);
}

async fn await_operation_completion(
    receiver: oneshot::Receiver<OperationFinishOutcome>,
) -> OperationFinishOutcome {
    receiver.await.unwrap_or_else(|_| {
        OperationFinishOutcome::Failed(ClientError::internal().status_message())
    })
}

fn operation_completion_outcome(completion: &OperationCompletion) -> OperationFinishOutcome {
    match &completion.result {
        Ok(_) => OperationFinishOutcome::Succeeded,
        Err(error) => OperationFinishOutcome::Failed(error.status_message()),
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
    get_info_types: Vec<i32>,
}

#[derive(Debug)]
struct CloseSessionReq {
    session_handle: Option<Handle>,
}

#[derive(Debug)]
struct GetInfoReq {
    session_handle: Option<Handle>,
    info_type: i32,
}

#[derive(Debug)]
struct GetColumnsReq {
    session_handle: Option<Handle>,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    column_name: Option<String>,
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
    let mut get_info_types = Vec::new();
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1281, T_LIST) => get_info_types = read_i32_list(reader)?,
            (1284, T_STRUCT) => {
                let namespace = read_namespace(reader)?;
                catalog = namespace.0;
                schema = namespace.1;
            }
            _ => reader.skip(field_type)?,
        }
    }
    Ok(OpenSessionReq {
        catalog,
        schema,
        get_info_types,
    })
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

fn read_i32_list(reader: &mut Reader<'_>) -> Result<Vec<i32>> {
    let element_type = reader.read_u8()?;
    let len = reader.read_i32()?;
    if len < 0 {
        return Err(HarborError::Thrift("negative i32 list size".into()));
    }
    if element_type != T_I32 {
        for _ in 0..len {
            reader.skip(element_type)?;
        }
        return Ok(Vec::new());
    }

    let mut values = Vec::with_capacity(len as usize);
    for _ in 0..len {
        values.push(reader.read_i32()?);
    }
    Ok(values)
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

fn read_get_info_req(reader: &mut Reader<'_>) -> Result<GetInfoReq> {
    let mut session_handle = None;
    let mut info_type = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => session_handle = read_session_handle(reader)?,
            (2, T_I32) => info_type = Some(reader.read_i32()?),
            _ => reader.skip(field_type)?,
        }
    }
    Ok(GetInfoReq {
        session_handle,
        info_type: info_type.unwrap_or(CLI_DBMS_NAME),
    })
}

fn read_get_columns_req(reader: &mut Reader<'_>) -> Result<GetColumnsReq> {
    let mut session_handle = None;
    let mut catalog_name = None;
    let mut schema_name = None;
    let mut table_name = None;
    let mut column_name = None;
    loop {
        let (field_type, field_id) = reader.read_field_begin()?;
        if field_type == T_STOP {
            break;
        }
        match (field_id, field_type) {
            (1, T_STRUCT) => session_handle = read_session_handle(reader)?,
            (2, T_STRING) => catalog_name = Some(reader.read_string()?),
            (3, T_STRING) => schema_name = Some(reader.read_string()?),
            (4, T_STRING) => table_name = Some(reader.read_string()?),
            (5, T_STRING) => column_name = Some(reader.read_string()?),
            _ => reader.skip(field_type)?,
        }
    }
    Ok(GetColumnsReq {
        session_handle,
        catalog_name,
        schema_name,
        table_name,
        column_name,
    })
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
    get_info_types: &[i32],
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
        if !get_info_types.is_empty() {
            writer.write_field(T_LIST, 1281, |writer| {
                writer.write_list_begin(T_STRUCT, get_info_types.len());
                for info_type in get_info_types {
                    write_get_info_value(writer, *info_type, catalog);
                }
            });
        }
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

fn write_get_info_response(seqid: i32, info_type: i32, catalog: &str) -> Vec<u8> {
    write_success_response("GetInfo", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_field(T_STRUCT, 2, |writer| {
            write_get_info_value(writer, info_type, catalog);
        });
        writer.write_stop();
    })
}

fn write_get_info_invalid(seqid: i32) -> Vec<u8> {
    write_success_response("GetInfo", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(
                writer,
                INVALID_HANDLE_STATUS,
                Some("invalid session handle"),
            )
        });
        writer.write_field(T_STRUCT, 2, |writer| {
            write_get_info_string_value(writer, "");
        });
        writer.write_stop();
    })
}

fn write_get_columns_response(seqid: i32, guid: &[u8; 16], secret: &[u8; 16]) -> Vec<u8> {
    write_success_response("GetColumns", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, SUCCESS_STATUS, None)
        });
        writer.write_field(T_STRUCT, 2, |writer| {
            write_operation_handle_with_type(writer, guid, secret, true, GET_COLUMNS);
        });
        writer.write_stop();
    })
}

fn write_get_columns_error(seqid: i32, message: &str) -> Vec<u8> {
    write_success_response("GetColumns", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, ERROR_STATUS, Some(message))
        });
        writer.write_stop();
    })
}

fn write_get_columns_invalid(seqid: i32, message: &str) -> Vec<u8> {
    write_success_response("GetColumns", seqid, |writer| {
        writer.write_field(T_STRUCT, 1, |writer| {
            write_status(writer, INVALID_HANDLE_STATUS, Some(message))
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

fn write_get_query_id_response(seqid: i32, query_id: &str) -> Vec<u8> {
    write_success_response("GetQueryId", seqid, |writer| {
        writer.write_field(T_STRING, 1, |writer| writer.write_string(query_id));
        writer.write_stop();
    })
}

fn write_get_result_set_metadata_response(seqid: i32, result: &QueryResult) -> Vec<u8> {
    write_success_response("GetResultSetMetadata", seqid, |writer| {
        if let Err(err) = write_result_set_metadata_response(writer, result, SUCCESS_STATUS) {
            let message = thrift_client_error_message(&err, "thrift result metadata encoding");
            write_result_set_metadata_response_with_error(writer, ERROR_STATUS, &message);
        }
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
        if let Err(err) = write_fetch_results_response_struct(
            writer,
            result,
            include_metadata,
            start_row_offset,
            max_rows,
        ) {
            let message = thrift_client_error_message(&err, "thrift result value encoding");
            writer.write_field(T_STRUCT, 1, |writer| {
                write_status(writer, ERROR_STATUS, Some(&message));
            });
            writer.write_field(T_BOOL, 2, |writer| writer.write_bool(false));
            writer.write_stop();
        }
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

fn thrift_client_error_message(error: &HarborError, context: &'static str) -> String {
    error.log_internal(context);
    error.client_error().status_message()
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
    write_operation_handle_with_type(writer, guid, secret, has_result_set, EXECUTE_STATEMENT);
}

fn write_operation_handle_with_type(
    writer: &mut Writer,
    guid: &[u8; 16],
    secret: &[u8; 16],
    has_result_set: bool,
    operation_type: i32,
) {
    writer.write_field(T_STRUCT, 1, |writer| {
        write_handle_identifier(writer, guid, secret);
    });
    writer.write_field(T_I32, 2, |writer| writer.write_i32(operation_type));
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

fn write_get_info_value(writer: &mut Writer, info_type: i32, catalog: &str) {
    match info_type {
        CLI_DATA_SOURCE_NAME
        | CLI_SERVER_NAME
        | CLI_SEARCH_PATTERN_ESCAPE
        | CLI_DBMS_NAME
        | CLI_DBMS_VER
        | CLI_IDENTIFIER_QUOTE_CHAR
        | CLI_USER_NAME
        | CLI_CATALOG_NAME
        | CLI_COLLATION_SEQ
        | CLI_SPECIAL_CHARACTERS => {
            write_get_info_string_value(writer, get_info_string(info_type, catalog));
        }
        CLI_MAX_DRIVER_CONNECTIONS
        | CLI_MAX_CONCURRENT_ACTIVITIES
        | CLI_MAX_COLUMN_NAME_LEN
        | CLI_MAX_CURSOR_NAME_LEN
        | CLI_MAX_SCHEMA_NAME_LEN
        | CLI_MAX_CATALOG_NAME_LEN
        | CLI_MAX_TABLE_NAME_LEN
        | CLI_MAX_INDEX_SIZE
        | CLI_MAX_ROW_SIZE
        | CLI_MAX_STATEMENT_LEN
        | CLI_MAX_TABLES_IN_SELECT
        | CLI_MAX_USER_NAME_LEN
        | CLI_MAX_IDENTIFIER_LEN => {
            writer.write_field(T_I64, 6, |writer| writer.write_i64(get_info_len(info_type)));
            writer.write_stop();
        }
        CLI_FETCH_DIRECTION
        | CLI_CURSOR_COMMIT_BEHAVIOR
        | CLI_DATA_SOURCE_READ_ONLY
        | CLI_DEFAULT_TXN_ISOLATION
        | CLI_IDENTIFIER_CASE
        | CLI_ACCESSIBLE_TABLES
        | CLI_ACCESSIBLE_PROCEDURES
        | CLI_TXN_CAPABLE
        | CLI_INTEGRITY
        | CLI_NULL_COLLATION
        | CLI_ORDER_BY_COLUMNS_IN_SELECT
        | CLI_XOPEN_CLI_YEAR
        | CLI_CURSOR_SENSITIVITY
        | CLI_DESCRIBE_PARAMETER => {
            writer.write_field(T_I32, 4, |writer| {
                writer.write_i32(get_info_flag(info_type))
            });
            writer.write_stop();
        }
        CLI_TXN_ISOLATION_OPTION
        | CLI_GETDATA_EXTENSIONS
        | CLI_ALTER_TABLE
        | CLI_MAX_COLUMNS_IN_GROUP_BY
        | CLI_MAX_COLUMNS_IN_INDEX
        | CLI_MAX_COLUMNS_IN_ORDER_BY
        | CLI_MAX_COLUMNS_IN_SELECT
        | CLI_MAX_COLUMNS_IN_TABLE
        | CLI_OJ_CAPABILITIES => {
            writer.write_field(T_I32, 3, |writer| writer.write_i32(0));
            writer.write_stop();
        }
        _ => {
            writer.write_field(T_I32, 3, |writer| writer.write_i32(0));
            writer.write_stop();
        }
    }
}

fn write_get_info_string_value(writer: &mut Writer, value: &str) {
    writer.write_field(T_STRING, 1, |writer| writer.write_string(value));
    writer.write_stop();
}

fn get_info_string(info_type: i32, catalog: &str) -> &str {
    match info_type {
        CLI_DATA_SOURCE_NAME => "HarborSQL",
        CLI_SERVER_NAME => "HarborSQL",
        CLI_SEARCH_PATTERN_ESCAPE => "\\",
        CLI_DBMS_NAME => "Spark SQL",
        CLI_DBMS_VER => "3.1.0",
        CLI_IDENTIFIER_QUOTE_CHAR => "`",
        CLI_USER_NAME => "",
        CLI_CATALOG_NAME => catalog,
        CLI_COLLATION_SEQ => "",
        CLI_SPECIAL_CHARACTERS => "",
        _ => "",
    }
}

fn get_info_len(info_type: i32) -> i64 {
    match info_type {
        CLI_MAX_COLUMN_NAME_LEN
        | CLI_MAX_CURSOR_NAME_LEN
        | CLI_MAX_SCHEMA_NAME_LEN
        | CLI_MAX_CATALOG_NAME_LEN
        | CLI_MAX_TABLE_NAME_LEN
        | CLI_MAX_USER_NAME_LEN
        | CLI_MAX_IDENTIFIER_LEN => 128,
        CLI_MAX_STATEMENT_LEN => 0,
        _ => 0,
    }
}

fn get_info_flag(info_type: i32) -> i32 {
    match info_type {
        CLI_FETCH_DIRECTION => 1,
        CLI_DATA_SOURCE_READ_ONLY => 1,
        CLI_IDENTIFIER_CASE => 1,
        CLI_ORDER_BY_COLUMNS_IN_SELECT => 1,
        CLI_XOPEN_CLI_YEAR => 2011,
        _ => 0,
    }
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
