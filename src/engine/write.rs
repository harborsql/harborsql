//! Deliberately narrow dbt CTAS support. Other DDL stays fail-closed.
use datafusion::{arrow::datatypes::DataType, prelude::SessionContext};
use deltalake::{DeltaTableBuilder, DeltaTableError, protocol::SaveMode};
use serde_json::{Value, json};
use sqlparser::{dialect::GenericDialect, keywords::Keyword, parser::Parser, tokenizer::Token};
use url::Url;

use super::{QueryEngine, QueryResult, catalog::storage_options};
use crate::error::{HarborError, Result};

pub(super) struct Ctas {
    pub name: [String; 3],
    pub location: String,
    pub query: String,
}

fn unsupported(message: impl Into<String>) -> HarborError {
    HarborError::UnsupportedSql(message.into())
}

pub(super) fn parse(sql: &str, catalog: &str, schema: &str) -> Result<Option<Ctas>> {
    let dialect = GenericDialect;
    let mut p = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| unsupported(e.to_string()))?;
    if !p.parse_keyword(Keyword::CREATE) {
        return Ok(None);
    }
    let parse_error = |e: sqlparser::parser::ParserError| unsupported(e.to_string());
    p.expect_keywords(&[Keyword::OR, Keyword::REPLACE, Keyword::TABLE])
        .map_err(parse_error)?;
    let object = p.parse_object_name(false).map_err(parse_error)?;
    let parts = object
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|id| id.value.clone())
                .ok_or_else(|| unsupported("CTAS requires a plain table name"))
        })
        .collect::<Result<Vec<_>>>()?;
    let name = match parts.as_slice() {
        [table] => [catalog.into(), schema.into(), table.clone()],
        [schema, table] => [catalog.into(), schema.clone(), table.clone()],
        [catalog, schema, table] => [catalog.clone(), schema.clone(), table.clone()],
        _ => {
            return Err(unsupported(
                "CTAS requires a one-, two-, or three-part name",
            ));
        }
    };
    if name.iter().any(|s| s.is_empty() || s.contains('.'))
        || name[0].eq_ignore_ascii_case("system")
        || name[1].eq_ignore_ascii_case("information_schema")
    {
        return Err(unsupported("unsupported CTAS target namespace"));
    }
    p.expect_keyword(Keyword::USING).map_err(parse_error)?;
    if !p
        .parse_identifier()
        .map_err(parse_error)?
        .value
        .eq_ignore_ascii_case("delta")
    {
        return Err(unsupported("CTAS only supports USING DELTA"));
    }
    if !p.parse_keyword(Keyword::LOCATION) {
        return Err(unsupported(
            "managed Delta CTAS requires Unity catalog commits; this writer only supports external tables with LOCATION",
        ));
    }
    let location = p.parse_literal_string().map_err(parse_error)?;
    validate_location(&location)?;
    p.expect_keyword(Keyword::AS).map_err(parse_error)?;
    let query = p.parse_query().map_err(parse_error)?.to_string();
    let _ = p.consume_token(&Token::SemiColon);
    p.expect_token(&Token::EOF).map_err(parse_error)?;
    Ok(Some(Ctas {
        name,
        location,
        query,
    }))
}

fn validate_location(location: &str) -> Result<Url> {
    let url = Url::parse(location)?;
    if url.scheme() != "s3"
        || url.host_str().is_none()
        || url.path().trim_matches('/').is_empty()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path().contains('%')
        || url.path().contains("//")
        || location.split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err(unsupported(
            "external CTAS requires an unambiguous s3://bucket/table-prefix LOCATION",
        ));
    }
    Ok(url)
}

impl QueryEngine {
    pub(super) async fn execute_ctas(
        &self,
        token: &str,
        ctas: Ctas,
        ctx: &SessionContext,
        dataframe: datafusion::dataframe::DataFrame,
        routes: &super::catalog::ObjectStoreRouteRegistry,
    ) -> Result<QueryResult> {
        let [catalog, schema, name] = &ctas.name;
        let full_name = ctas.name.join(".");
        // List success is required: never interpret permission/network errors as absence.
        let existing = self
            .unity
            .tables(token, catalog, schema)
            .await?
            .into_iter()
            .any(|t| {
                t.name
                    .as_deref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(name))
            });
        let columns = unity_columns(dataframe.schema().as_arrow())?;
        let credentials = if existing {
            let details = self.unity.table_details(token, &full_name).await?;
            validate_existing(&details, &ctas.location, &columns)?;
            let id = details["table_id"]
                .as_str()
                .ok_or_else(|| unsupported("Unity target is missing table_id"))?;
            self.unity.write_credentials(token, id).await?
        } else {
            self.unity
                .create_path_credentials(token, &ctas.location)
                .await?
        };
        if credentials.url.trim_end_matches('/') != ctas.location.trim_end_matches('/') {
            return Err(unsupported(
                "Unity write credential URL does not match CTAS LOCATION",
            ));
        }
        let mut table = DeltaTableBuilder::from_url(validate_location(&ctas.location)?)?
            .with_storage_options(storage_options(&credentials, &self.config.aws_region))
            .build()?;
        match table.load().await {
            Ok(()) if !existing => {
                return Err(unsupported(
                    "CTAS location already contains an unregistered Delta table; recover registration explicitly before retrying",
                ));
            }
            Ok(()) => {}
            Err(DeltaTableError::NotATable(_)) if !existing => {}
            Err(e) => return Err(e.into()),
        }
        let mode = if existing {
            SaveMode::Overwrite
        } else {
            SaveMode::ErrorIfExists
        };
        let (url, prefix) = super::catalog::table_object_store_route(&ctas.location)?;
        routes.record_store(url, prefix, table.log_store().root_object_store(None))?;
        for (url, stores) in routes.routes()? {
            ctx.register_object_store(
                &url,
                std::sync::Arc::new(super::PrefixRoutingObjectStore::new(stores)),
            );
        }
        // Invalidation also runs on cancellation / ambiguous commit outcome.
        let _invalidate = InvalidateOnDrop {
            cache: self.table_cache.clone(),
            name: full_name,
        };
        table
            .write([])
            .with_input_plan(dataframe.logical_plan().clone())
            .with_session_state(std::sync::Arc::new(ctx.state()))
            .with_save_mode(mode)
            .await?;
        if !existing {
            self.unity.create_external_table(token, &json!({
                "catalog_name": catalog, "schema_name": schema, "name": name,
                "table_type": "EXTERNAL", "data_source_format": "DELTA",
                "storage_location": ctas.location, "columns": columns,
            })).await.map_err(|e| HarborError::Query(format!("Delta data committed but Unity registration failed; do not delete the data; recover registration before retrying: {}", e.redacted_internal_message())))?;
        }
        Ok(QueryResult::empty())
    }
}

struct InvalidateOnDrop {
    cache: crate::table_cache::TableCache,
    name: String,
}
impl Drop for InvalidateOnDrop {
    fn drop(&mut self) {
        if self.cache.invalidate_table(&self.name).is_err() {
            tracing::error!("failed to invalidate a written table's cache");
        }
    }
}

fn validate_existing(details: &Value, location: &str, columns: &[Value]) -> Result<()> {
    if details["table_type"] != "EXTERNAL"
        || details["data_source_format"] != "DELTA"
        || details["storage_location"]
            .as_str()
            .map(|s| s.trim_end_matches('/'))
            != Some(location.trim_end_matches('/'))
    {
        return Err(unsupported(
            "replacement requires an existing EXTERNAL DELTA table at exactly the requested LOCATION",
        ));
    }
    if !details["row_filter"].is_null()
        || details["columns"]
            .as_array()
            .is_some_and(|cols| cols.iter().any(|c| !c["mask"].is_null()))
    {
        return Err(unsupported(
            "replacing tables with row filters or column masks is not supported",
        ));
    }
    let old = details["columns"]
        .as_array()
        .ok_or_else(|| unsupported("Unity target is missing columns"))?;
    if old.len() != columns.len()
        || old.iter().zip(columns).any(|(a, b)| {
            a["name"] != b["name"]
                || a["type_text"].as_str().map(str::to_lowercase)
                    != b["type_text"].as_str().map(str::to_lowercase)
        })
    {
        return Err(unsupported(
            "CTAS replacement currently requires unchanged column names and types",
        ));
    }
    Ok(())
}

fn unity_columns(schema: &datafusion::arrow::datatypes::Schema) -> Result<Vec<Value>> {
    schema.fields().iter().enumerate().map(|(position, field)| {
        let (name, text) = match field.data_type() {
            DataType::Int8 => ("BYTE", "tinyint".into()),
            DataType::Int16 => ("SHORT", "smallint".into()),
            DataType::Int32 => ("INT", "int".into()),
            DataType::Int64 => ("LONG", "bigint".into()),
            DataType::Float32 => ("FLOAT", "float".into()),
            DataType::Float64 => ("DOUBLE", "double".into()),
            DataType::Boolean => ("BOOLEAN", "boolean".into()),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ("STRING", "string".into()),
            DataType::Date32 => ("DATE", "date".into()),
            DataType::Timestamp(_, None) => ("TIMESTAMP_NTZ", "timestamp_ntz".into()),
            DataType::Timestamp(_, Some(_)) => ("TIMESTAMP", "timestamp".into()),
            DataType::Decimal128(p, s) => ("DECIMAL", format!("decimal({p},{s})")),
            other => return Err(unsupported(format!("CTAS Unity column type not supported: {other}"))),
        };
        let json_type = match name {
            "BYTE" => "byte", "SHORT" => "short", "INT" => "integer", "LONG" => "long",
            _ => &text,
        };
        Ok(json!({"name": field.name(), "position": position, "type_name": name,
            "type_text": text, "nullable": true,
            "type_json": json!({"name": field.name(), "type": json_type, "nullable": true, "metadata": {}}).to_string()}))
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn overwrite_is_atomic_on_evaluation_failure_and_retry_handles_empty_results() {
        let ctx = deltalake::delta_datafusion::create_session().into_inner();
        crate::udf::register_udfs(&ctx);
        let table = DeltaTableBuilder::from_url(Url::parse("memory:///ctas-test").unwrap())
            .unwrap()
            .build()
            .unwrap();
        let initial = ctx.sql("select cast(1 as int) as id").await.unwrap();
        let mut table = table
            .write([])
            .with_input_plan(initial.logical_plan().clone())
            .with_session_state(std::sync::Arc::new(ctx.state()))
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        let version = table.version();
        let failed = ctx
            .sql("select cast(raise_error('controlled') as int) as id")
            .await
            .unwrap();
        assert!(
            table
                .clone()
                .write([])
                .with_input_plan(failed.logical_plan().clone())
                .with_session_state(std::sync::Arc::new(ctx.state()))
                .with_save_mode(SaveMode::Overwrite)
                .await
                .is_err()
        );
        table.load().await.unwrap();
        assert_eq!(table.version(), version);
        let batches = ctx
            .read_table(table.table_provider().await.unwrap())
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let empty = ctx
            .sql("select cast(2 as int) as id where false")
            .await
            .unwrap();
        table = table
            .write([])
            .with_input_plan(empty.logical_plan().clone())
            .with_session_state(std::sync::Arc::new(ctx.state()))
            .with_save_mode(SaveMode::Overwrite)
            .await
            .unwrap();
        assert_ne!(table.version(), version);
        let batches = ctx
            .read_table(table.table_provider().await.unwrap())
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    #[test]
    fn validates_target_type_location_policies_and_schema() {
        let columns = vec![json!({"name":"id", "type_text":"int"})];
        let base = json!({"table_type":"EXTERNAL", "data_source_format":"DELTA", "storage_location":"s3://bucket/t", "columns": columns});
        validate_existing(&base, "s3://bucket/t/", &columns).unwrap();
        for (key, value) in [
            ("table_type", json!("MANAGED")),
            ("storage_location", json!("s3://bucket/other")),
            ("row_filter", json!({"function_name":"c.s.filter"})),
            ("columns", json!([{"name":"id", "type_text":"bigint"}])),
            (
                "columns",
                json!([{"name":"id", "type_text":"int", "mask":{}}]),
            ),
        ] {
            let mut details = base.clone();
            details[key] = value;
            assert!(validate_existing(&details, "s3://bucket/t", &columns).is_err());
        }
    }

    #[test]
    fn parses_dbt_ctas_and_rejects_unimplemented_clauses() {
        let statement = "/* dbt */ create or replace table `c`.`s`.`t` using delta location 's3://bucket/table' as select 1 as id;";
        let ctas = parse(statement, "default", "default").unwrap().unwrap();
        assert_eq!(ctas.name, ["c", "s", "t"]);
        assert_eq!(ctas.query, "SELECT 1 AS id");
        for sql in [
            "create or replace table t using delta as select 1",
            "create or replace table t using delta tblproperties ('delta.feature.catalogManaged'='supported') as select 1",
            "create or replace table t using delta location 's3://bucket/t' partition by id as select 1",
            "create or replace table t using parquet location 's3://bucket/t' as select 1",
            "create or replace table t using delta location 's3://bucket/t' as select 1; drop table other",
            "create or replace table t using delta location 'file:///tmp/t' as select 1",
        ] {
            assert!(parse(sql, "c", "s").is_err(), "{sql}");
        }
        assert!(parse("select 1", "c", "s").unwrap().is_none());
    }
}
