# HarborSQL

HarborSQL is an experimental external SQL query engine for Unity Catalog Delta tables.

It accepts Databricks SQL connector-style requests, forwards the client's Databricks bearer token to Unity Catalog, vends short-lived table credentials, opens Delta data with `delta-rs`, executes read-only SQL with DataFusion, and returns Databricks-compatible result sets.

## Status

This repository is an early proof of concept. The current implementation is intentionally narrow:

- read-only `SELECT` queries
- Unity Catalog Delta tables
- AWS S3-backed table credentials
- Databricks SQL connector compatibility for a minimal Thrift-over-HTTP surface
- inline result sets, without Databricks Cloud Fetch

It is not production-ready. Important missing pieces include result pagination limits, async operation lifecycle, cancellation, stronger protocol tests, broader SQL compatibility, and hardened operational controls.

## How It Works

For each query, HarborSQL:

1. Reads the bearer token from the Databricks SQL client request.
2. Uses that token to call Unity Catalog.
3. Resolves referenced Delta tables.
4. Requests temporary table credentials from Unity Catalog.
5. Registers Delta table providers in DataFusion.
6. Executes the SQL query locally.
7. Returns rows through the Databricks SQL connector protocol.

Authorization remains anchored in Unity Catalog. HarborSQL does not persist Databricks bearer tokens or temporary cloud credentials.

## Requirements

- Rust `1.91+`
- Access to a Databricks workspace with Unity Catalog enabled
- A Unity Catalog Delta table that can vend temporary table credentials to external clients
- AWS credentials/permissions handled through Unity Catalog temporary table credentials

## Configuration

HarborSQL reads configuration from environment variables:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `HARBORSQL_DATABRICKS_HOST` or `DATABRICKS_HOST` | yes | none | Databricks workspace URL or host |
| `HARBORSQL_BIND_ADDR` | no | `127.0.0.1:1992` | HTTP bind address |
| `HARBORSQL_DEFAULT_CATALOG` or `DATABRICKS_CATALOG` | no | `workspace` | Default catalog for unqualified queries |
| `HARBORSQL_DEFAULT_SCHEMA` or `DATABRICKS_SCHEMA` | no | `default` | Default schema for unqualified queries |
| `HARBORSQL_AWS_REGION` | no | `us-west-2` | AWS region passed to Delta object-store access |
| `DATABRICKS_TOKEN` | query mode only | none | Token used by `harborsql query --sql ...` |

## Run The Server

```bash
export HARBORSQL_DATABRICKS_HOST="https://<workspace-host>"
export HARBORSQL_DEFAULT_CATALOG="<catalog>"
export HARBORSQL_DEFAULT_SCHEMA="<schema>"
export HARBORSQL_AWS_REGION="us-west-2"

cargo run -- server
```

The server listens on `127.0.0.1:1992` by default.

## Run A One-Off Query

```bash
export HARBORSQL_DATABRICKS_HOST="https://<workspace-host>"
export DATABRICKS_TOKEN="<token>"

cargo run -- query --sql "SELECT COUNT(*) FROM <catalog>.<schema>.<table>"
```

## Local Databricks SQL Connector Use

For local HTTP development, the Python Databricks SQL connector needs its private `_connection_uri` override because normal connector settings assume HTTPS:

```python
from databricks import sql

connection = sql.connect(
    server_hostname="http://127.0.0.1:1992",
    http_path="/sql/1.0/warehouses/<warehouse-id>",
    access_token=token,
    catalog="<catalog>",
    schema="<schema>",
    _connection_uri="http://127.0.0.1:1992/sql/1.0/warehouses/<warehouse-id>",
    use_cloud_fetch=False,
)
```

Production deployments should serve HarborSQL over HTTPS and use normal connector settings.

## Tests

```bash
cargo test
```

## Benchmarks

Benchmark setup, Unity Catalog runbooks, topology notes, and result artifacts live outside this public engine repository in the separate benchmark repository:

```text
git@github.com:ablanchard/harborsql-bench.git
```

Keep environment-specific benchmark data, workspace identifiers, storage paths, and generated benchmark results out of this repository.

## Security Notes

- Do not log or persist bearer tokens.
- Do not log or persist temporary cloud credentials.
- Treat Unity Catalog as the authorization source of truth.
- Keep concrete workspace, schema, bucket, table, and credential identifiers in private runbooks.
