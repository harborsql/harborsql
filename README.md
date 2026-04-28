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

It is not production-ready. Important missing pieces include Cloud Fetch, durable or streaming result storage beyond in-memory materialized results, stronger protocol tests, broader SQL compatibility, and production-grade operational controls.

## How It Works

For each query, HarborSQL:

1. Reads the bearer token from the Databricks SQL client request.
2. Uses that token to call Unity Catalog.
3. Resolves referenced Delta tables.
4. Reuses an in-memory, token-scoped table cache when the caller already has
   fresh Unity temporary table credentials for that table.
5. Requests temporary table credentials from Unity Catalog on cache misses.
6. Registers Delta table providers in DataFusion.
7. Executes the SQL query locally.
8. Returns rows through the Databricks SQL connector protocol.

Authorization remains anchored in Unity Catalog. HarborSQL does not persist Databricks bearer tokens or temporary cloud credentials. Cached table entries are keyed by a process-local HMAC of the caller's bearer token and expire before Unity temporary credentials expire.

## Requirements

- Rust `1.91+`
- Access to a Databricks workspace with Unity Catalog enabled
- A Unity Catalog Delta table that can vend temporary table credentials to external clients
- AWS credentials/permissions handled through Unity Catalog temporary table credentials

## Configuration

HarborSQL reads configuration from environment variables:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `HARBORSQL_DATABRICKS_HOST` or `DATABRICKS_HOST` | yes | none | Databricks workspace URL or host; defaults to `https://` when no scheme is supplied and rejects `http://` unless explicitly allowed |
| `HARBORSQL_UNSAFE_ALLOW_HTTP_DATABRICKS_HOST` | no | `false` | Allows an `http://` Databricks host value for local non-Databricks test endpoints only; do not use with real Databricks bearer tokens |
| `HARBORSQL_BIND_ADDR` | no | `127.0.0.1:1992` | HTTP bind address |
| `HARBORSQL_DEFAULT_CATALOG` or `DATABRICKS_CATALOG` | no | `workspace` | Default catalog for unqualified queries |
| `HARBORSQL_DEFAULT_SCHEMA` or `DATABRICKS_SCHEMA` | no | `default` | Default schema for unqualified queries |
| `HARBORSQL_AWS_REGION` | no | `us-west-2` | AWS region passed to Delta object-store access |
| `HARBORSQL_MAX_RESULT_ROWS` | no | `100000` | Maximum rows HarborSQL will materialize for one query; set to an empty value to disable |
| `HARBORSQL_MAX_RESULT_BYTES` | no | `67108864` | Maximum retained Arrow result page bytes HarborSQL will materialize for one query; set to an empty value to disable |
| `HARBORSQL_UNITY_TIMEOUT_SECONDS` | no | `30` | Timeout for Unity Catalog HTTP requests |
| `HARBORSQL_QUERY_TIMEOUT_SECONDS` | no | `300` | Timeout for each query execution |
| `HARBORSQL_IDLE_SESSION_TIMEOUT_SECONDS` | no | `1800` | Idle timeout for Thrift sessions |
| `HARBORSQL_COMPLETED_OPERATION_TTL_SECONDS` | no | `600` | Retention time for completed Thrift operations and their materialized results |
| `HARBORSQL_CLEANUP_INTERVAL_SECONDS` | no | `60` | Background cleanup interval for expired sessions and operations |
| `HARBORSQL_MAX_SESSIONS` | no | `256` | Maximum concurrent Thrift sessions |
| `HARBORSQL_MAX_OPERATIONS` | no | `512` | Maximum retained Thrift operations |
| `HARBORSQL_REQUEST_BODY_LIMIT_BYTES` | no | `1048576` | Maximum HTTP request body size |
| `HARBORSQL_PARQUET_PUSHDOWN_FILTERS` | no | `true` | Enable DataFusion Parquet filter pushdown / late materialization; useful for wide filtered scans such as ClickBench q24 |
| `HARBORSQL_PARQUET_REORDER_FILTERS` | no | same as `HARBORSQL_PARQUET_PUSHDOWN_FILTERS` | Reorder pushed-down Parquet filters heuristically |
| `HARBORSQL_TARGET_PARTITIONS` | no | max of available CPU parallelism and `32` | DataFusion target partition count; higher values improve S3 scan concurrency for ordered-limit scans |
| `HARBORSQL_SKIP_PARTIAL_AGGREGATION_PROBE_ROWS_THRESHOLD` | no | `10000` | Rows per partition DataFusion samples before bypassing partial aggregation for high-cardinality group keys |
| `HARBORSQL_SKIP_PARTIAL_AGGREGATION_PROBE_RATIO_THRESHOLD` | no | `0.8` | Distinct-groups/input-rows ratio that triggers partial aggregation bypass |
| `HARBORSQL_TABLE_CACHE_TTL_SECONDS` | no | `300` | Maximum lifetime for token-scoped cached table providers; set to `0` to disable |
| `HARBORSQL_TABLE_CACHE_MAX_ENTRIES` | no | `1024` | Maximum token/table/region cache entries; set to `0` to disable |
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

Parquet late materialization is enabled by default. To disable it for comparison runs:

```bash
export HARBORSQL_PARQUET_PUSHDOWN_FILTERS=false
cargo run -- server
```

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

This local HTTP override is only for the client-to-HarborSQL hop. HarborSQL still requires its upstream `HARBORSQL_DATABRICKS_HOST` to use HTTPS by default because bearer tokens are forwarded to Unity Catalog. If a local test double really needs an HTTP upstream endpoint, set `HARBORSQL_UNSAFE_ALLOW_HTTP_DATABRICKS_HOST=true` and do not use real Databricks credentials.

Production deployments should serve HarborSQL over HTTPS, or behind a TLS-terminating proxy, and use normal connector settings. The HarborSQL-to-Databricks/Unity Catalog hop should always be HTTPS for real Databricks workspaces.

## Tests

```bash
cargo test
```

The local Databricks SQL connector smoke test is ignored by default because it
requires Python with `databricks-sql-connector` installed:

```bash
HARBORSQL_CONNECTOR_SMOKE_PYTHON=/path/to/python \
  cargo test --test databricks_connector_smoke -- --ignored
```

Set `HARBORSQL_CONNECTOR_SMOKE_AUTH` to choose the connector authentication path:

- `local` uses a synthetic local bearer token.
- `pat` uses `DATABRICKS_TOKEN` or `TEST_CI_DATABRICKS_PAT`.
- `oauth` uses the Databricks SQL connector OAuth machine-to-machine path.
- `auto` uses OAuth when client credentials are present, then PAT when a token
  is present, otherwise local mode.

OAuth mode reads the workspace host from `HARBORSQL_DATABRICKS_HOST`,
`DATABRICKS_HOST`, or `BENCH_US_DATABRICKS_HOSTNAME`.

The GitHub Actions workflow expects these repository secrets:

- `BENCH_US_DATABRICKS_HOSTNAME`
- `DATABRICKS_ACCOUNT_ID`
- `TEST_CI_DATABRICKS_CLIENT_ID`
- `TEST_CI_DATABRICKS_CLIENT_SECRET`
- `TEST_CI_DATABRICKS_PAT`

## Release Publishing

Publishing a GitHub release runs the release workflow and publishes:

- `ghcr.io/<owner>/harborsql:<tag>` as a Linux x86_64 Docker image
- `ghcr.io/<owner>/harborsql-binaries:<tag>` as an OCI package containing the Linux x86_64 binary archive, plus the macOS Apple Silicon archive when enabled
- the same binary archives as GitHub release assets

For non-prerelease GitHub releases, the macOS artifact is mandatory and the
workflow also updates the `latest` tags. GitHub prereleases skip the macOS
artifact by default; add `[build-macos]` to the prerelease notes to include it.
The Docker image is built from the already-compiled Linux binary, so the
release build does not compile the Rust code again inside Docker.

```bash
docker pull ghcr.io/<owner>/harborsql:<tag>
oras pull ghcr.io/<owner>/harborsql-binaries:<tag>
```

To run the pre-release benchmark gate without publishing, use the `Release`
workflow's manual `workflow_dispatch` trigger with `publish` disabled. The
manual trigger also has a `build_macos` option when a prerelease candidate needs
the macOS binary. The default benchmark command is:

```bash
cargo test --release --locked --all-targets
```

Override `benchmark_command` in the manual workflow run if the benchmark suite
lives in another repository or needs a different command.

GitHub Packages may create the first GHCR package as private. If this repository
is public and the images should be public, change the package visibility in the
GitHub package settings after the first publish.

The release workflow does not require Databricks secrets. Publishing to GHCR and
uploading release assets use the built-in `GITHUB_TOKEN`.

## Benchmarks

Benchmark setup, Unity Catalog runbooks, topology notes, and result artifacts live outside this public engine repository in the separate benchmark repository:

```text
git@github.com:ablanchard/harborsql-bench.git
```

Keep environment-specific benchmark data, workspace identifiers, storage paths, and generated benchmark results out of this repository.

## Security Notes

- Do not log or persist bearer tokens.
- Do not log or persist temporary cloud credentials.
- Keep `HARBORSQL_DATABRICKS_HOST` on HTTPS for real Databricks workspaces;
  the HTTP override is only for local non-Databricks test endpoints.
- Table cache entries are per bearer-token fingerprint, in-memory only, bounded,
  and expire before Unity temporary table credentials expire.
- Treat Unity Catalog as the authorization source of truth.
- Keep concrete workspace, schema, bucket, table, and credential identifiers in private runbooks.
