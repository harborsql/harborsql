use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::task::{JoinError, JoinHandle};

use crate::{
    engine::QueryResult,
    error::{ClientError, redact_sensitive},
};

use super::protocol::*;

#[derive(Debug, Serialize)]
pub struct QueryHistory {
    pub query_id: String,
    pub status: String,
    pub duration: u64,
}

#[derive(Debug, Clone)]
pub(super) struct SessionState {
    pub(super) id: String,
    pub(super) secret: [u8; 16],
    pub(super) token_fingerprint: [u8; 32],
    pub(super) catalog: String,
    pub(super) schema: String,
    pub(super) last_access: Instant,
}

impl SessionState {
    pub(super) fn is_expired(&self, now: Instant, idle_timeout: Duration) -> bool {
        now.saturating_duration_since(self.last_access) >= idle_timeout
    }
}

pub(super) struct OperationState {
    pub(super) secret: [u8; 16],
    pub(super) session_id: String,
    pub(super) token_fingerprint: [u8; 32],
    pub(super) state: OperationExecution,
}

impl OperationState {
    pub(super) fn is_active(&self) -> bool {
        self.state.is_active()
    }

    pub(super) fn has_finished_task(&self) -> bool {
        matches!(&self.state, OperationExecution::Running { task, .. } if task.is_finished())
    }

    pub(super) fn take_finished_task(
        &mut self,
    ) -> Option<(Instant, JoinHandle<OperationCompletion>)> {
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

    pub(super) fn finish_refresh(
        &mut self,
        started: Instant,
        result: std::result::Result<OperationCompletion, JoinError>,
    ) {
        if matches!(self.state, OperationExecution::Refreshing { .. }) {
            self.state = OperationExecution::from_join_result(started, result);
        }
    }

    pub(super) fn abort(&self) {
        if let OperationExecution::Running { task, .. } = &self.state {
            task.abort();
        }
    }

    pub(super) fn cancel(&mut self) {
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

    pub(super) fn is_expired(&self, now: Instant, completed_operation_ttl: Duration) -> bool {
        self.state.is_expired(now, completed_operation_ttl)
    }
}

pub(super) enum OperationExecution {
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
    pub(super) fn is_active(&self) -> bool {
        matches!(self, Self::Running { .. } | Self::Refreshing { .. })
    }

    pub(super) fn from_completion(completion: OperationCompletion) -> Self {
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

    pub(super) fn result(&self) -> Option<&QueryResult> {
        match self {
            Self::Finished { result, .. } => Some(result.as_ref()),
            _ => None,
        }
    }

    pub(super) fn status_code(&self) -> i32 {
        match self {
            Self::Failed { .. } => ERROR_STATUS,
            _ => SUCCESS_STATUS,
        }
    }

    pub(super) fn operation_state(&self) -> i32 {
        match self {
            Self::Running { .. } | Self::Refreshing { .. } => RUNNING_STATE,
            Self::Finished { .. } => FINISHED_STATE,
            Self::Failed { .. } => ERROR_STATE,
            Self::Canceled { .. } => CANCELED_STATE,
        }
    }

    pub(super) fn duration_ms(&self) -> u64 {
        match self {
            Self::Running { started, .. } | Self::Refreshing { started } => {
                started.elapsed().as_millis() as u64
            }
            Self::Finished { duration_ms, .. }
            | Self::Failed { duration_ms, .. }
            | Self::Canceled { duration_ms, .. } => *duration_ms,
        }
    }

    pub(super) fn history_status(&self) -> &'static str {
        match self {
            Self::Running { .. } | Self::Refreshing { .. } => "RUNNING",
            Self::Finished { .. } => "FINISHED",
            Self::Failed { .. } => "FAILED",
            Self::Canceled { .. } => "CANCELED",
        }
    }

    pub(super) fn error_message(&self) -> Option<String> {
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

pub(super) struct OperationCompletion {
    pub(super) duration_ms: u64,
    pub(super) result: std::result::Result<QueryResult, ClientError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Handle {
    pub(super) id: String,
    pub(super) secret: [u8; 16],
}

pub(super) fn token_fingerprint(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}
