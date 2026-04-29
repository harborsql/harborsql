use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;

use crate::error::{HarborError, Result};

use super::{Column, QueryResult};

#[derive(Debug, Clone, Copy)]
pub(super) struct ResultLimits {
    pub max_rows: Option<usize>,
    pub max_bytes: Option<usize>,
}

pub(super) async fn materialize_stream(
    mut stream: SendableRecordBatchStream,
    limits: ResultLimits,
) -> Result<QueryResult> {
    let stream_schema = stream.schema();
    let fields = stream_schema.fields();
    let schema = fields
        .iter()
        .map(|field| Column {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect();
    let data_types = fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect();

    let mut row_count = 0;
    let mut result_bytes = 0_usize;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        row_count += batch.num_rows();
        if let Some(max_rows) = limits.max_rows
            && row_count > max_rows
        {
            return Err(HarborError::Query(format!(
                "query returned more than HARBORSQL_MAX_RESULT_ROWS={max_rows}",
            )));
        }

        result_bytes = result_bytes.saturating_add(batch.get_array_memory_size());
        if let Some(max_bytes) = limits.max_bytes
            && result_bytes > max_bytes
        {
            return Err(HarborError::Query(format!(
                "query result Arrow pages exceeded HARBORSQL_MAX_RESULT_BYTES={max_bytes}",
            )));
        }
        batches.push(batch);
    }

    Ok(QueryResult::from_batches_with_data_types(
        schema, data_types, batches,
    ))
}
