# Experimental external Delta writes

Set `HARBORSQL_ENABLE_EXTERNAL_WRITES=true` to enable the narrow table
materialization shape emitted by dbt-databricks's default materialization:

```sql
CREATE OR REPLACE TABLE <catalog>.<schema>.<table>
USING DELTA
LOCATION 's3://<bucket>/<table-prefix>'
AS SELECT ...;
```

Other DDL and DML remain unsupported. This is not general dbt compatibility.
The implementation uses DataFusion locally and delta-rs for the Delta commit;
it never delegates SQL execution to a Databricks warehouse.

## Authorization and registration

Fresh tables use Unity `PATH_CREATE_TABLE` credentials, write a new Delta
version, then register an `EXTERNAL` table through the Unity tables API.
Existing targets must be external Delta tables at the same location with the
same column names and types. They require Unity `READ_WRITE` table credentials.
Read credentials are never used as a fallback for writes. Credential routing
remains scoped to each table prefix, including when source and target share a
bucket. Set `HARBORSQL_AWS_REGION` to the actual S3 bucket region.

Unity grants still apply, including `EXTERNAL USE SCHEMA`, `CREATE TABLE`,
`CREATE EXTERNAL TABLE`, `EXTERNAL USE LOCATION`, and `MODIFY` as appropriate.
See [Databricks external access administration](https://docs.databricks.com/aws/en/external-access/admin)
and [credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending).

## Transaction and recovery boundaries

Replacement is one Delta overwrite transaction, not an empty-table commit
followed by an insert. Evaluation failure before commit preserves the old
version. A lost acknowledgement or timeout can still mean the commit succeeded;
inspect the target before deciding whether to retry. Cached target snapshots
are invalidated across principals on completion or cancellation. Already-running
reads may continue using their old snapshots.

Fresh creation is **not atomic across Delta storage and Unity registration**.
If registration fails after the Delta commit, data remains at the requested
location. HarborSQL refuses to overwrite an unregistered Delta table. An
operator must inspect and register that data before retrying. No automatic
cleanup deletes committed data or uncommitted files. Failed writes can leave
unreferenced files; apply normal Delta retention and vacuum practices.

## Unsupported cases

- Managed tables and `catalogManaged`: they require Unity catalog commits,
  which this writer does not implement. No direct-storage workaround is used.
- Incremental `MERGE`, insert, schema changes, table properties, partitioning,
  constraints, changing locations, and replacing views or clones.
- Targets with row filters or column masks. The `row_filters` metadata relation
  reads Unity table details and fails on unsupported policy argument formats.
- Non-S3 storage and complex output column types.
- Automatic credential refresh during a long-running write.
- Cross-system atomic creation and automatic recovery of failed registration.

Output streams go directly to the Delta writer, not through the result-row
materialization limit. Query timeout still applies; result limits do not impose
a write-size limit. Enable this only for trusted workloads with suitable
compute and storage quotas. This remains experimental pending broader race,
cancellation, and connector coverage.
