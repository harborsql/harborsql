# HarborSQL

HarborSQL is an external SQL query engine for Unity Catalog Delta tables.

It accepts Databricks SQL connector-style requests, forwards the caller's
Databricks bearer token to Unity Catalog, vends short-lived table credentials,
opens Delta data with `delta-rs`, executes read-only SQL with DataFusion, and
returns Databricks-compatible result sets.

## Capabilities

- read-only `SELECT` queries
- Unity Catalog Delta table discovery and authorization
- AWS S3-backed temporary table credentials vended by Unity Catalog
- Databricks SQL connector compatibility for a focused Thrift-over-HTTP surface
- DataFusion query execution with Delta Lake table providers
- bounded in-memory result materialization
- structured tracing, request IDs, and Prometheus metrics
- stable client-facing error codes with centrally redacted internal logs

HarborSQL is pre-1.0 software. The compatibility surface is intentionally
defined and still growing. Cloud Fetch, durable/streaming result storage, and
broader Databricks SQL protocol coverage are not yet part of the default
runtime.

## How It Works

For each query, HarborSQL:

1. Reads the bearer token from the Databricks SQL client request.
2. Uses that token to call Unity Catalog.
3. Resolves referenced Delta tables lazily while DataFusion plans the query.
4. Reuses token-scoped cached table providers when fresh temporary credentials
   are already available.
5. Requests Unity Catalog temporary table credentials on cache misses.
6. Registers Delta table providers in DataFusion.
7. Executes the SQL query locally.
8. Returns rows through the Databricks SQL connector protocol.

Authorization remains anchored in Unity Catalog. HarborSQL does not persist
Databricks bearer tokens or temporary cloud credentials. Cached table entries
are keyed by a process-local HMAC of the caller's bearer token and expire before
Unity temporary credentials expire.

## Requirements

- Docker, for the published container image
- Rust `1.91+`, for running from source
- Access to a Databricks workspace with Unity Catalog enabled
- A Unity Catalog Delta table that can vend temporary table credentials to
  external clients
- Object storage access mediated through Unity Catalog temporary table
  credentials

## Quick Start

Run the published Docker image:

```bash
export TAG="<version>"

docker run --rm \
  -p 127.0.0.1:1992:1992 \
  -e HARBORSQL_BIND_ADDR="0.0.0.0:1992" \
  -e HARBORSQL_DATABRICKS_HOST="https://<workspace-host>" \
  ghcr.io/harborsql/harborsql:$TAG
```

`HARBORSQL_BIND_ADDR=0.0.0.0:1992` makes HarborSQL listen on the
container interface. The `-p 127.0.0.1:1992:1992` mapping exposes it only on
host localhost.

Add `HARBORSQL_DEFAULT_CATALOG`, `HARBORSQL_DEFAULT_SCHEMA`, or
`HARBORSQL_AWS_REGION` only when the defaults do not match your workspace.

Run the server from source:

```bash
export HARBORSQL_DATABRICKS_HOST="https://<workspace-host>"

cargo run -- server
```

The server listens on `127.0.0.1:1992` by default.

Run a one-off query:

```bash
export HARBORSQL_DATABRICKS_HOST="https://<workspace-host>"
export DATABRICKS_TOKEN="<token>"

cargo run -- query --sql "SELECT COUNT(*) FROM <catalog>.<schema>.<table>"
```

Run the same one-off query with Docker:

```bash
docker run --rm \
  -e HARBORSQL_DATABRICKS_HOST="https://<workspace-host>" \
  -e DATABRICKS_TOKEN="<token>" \
  ghcr.io/harborsql/harborsql:$TAG \
  query --sql "SELECT COUNT(*) FROM <catalog>.<schema>.<table>"
```

Production deployments should serve HarborSQL over HTTPS, or behind a
TLS-terminating proxy. The HarborSQL-to-Databricks/Unity Catalog hop must use
HTTPS for real Databricks workspaces.

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
| `HARBORSQL_PARQUET_PUSHDOWN_FILTERS` | no | `true` | Enable DataFusion Parquet filter pushdown / late materialization |
| `HARBORSQL_PARQUET_REORDER_FILTERS` | no | same as `HARBORSQL_PARQUET_PUSHDOWN_FILTERS` | Reorder pushed-down Parquet filters heuristically |
| `HARBORSQL_TARGET_PARTITIONS` | no | max of available CPU parallelism and `32` | DataFusion target partition count |
| `HARBORSQL_SKIP_PARTIAL_AGGREGATION_PROBE_ROWS_THRESHOLD` | no | `10000` | Rows per partition DataFusion samples before bypassing partial aggregation for high-cardinality group keys |
| `HARBORSQL_SKIP_PARTIAL_AGGREGATION_PROBE_RATIO_THRESHOLD` | no | `0.8` | Distinct-groups/input-rows ratio that triggers partial aggregation bypass |
| `HARBORSQL_TABLE_CACHE_TTL_SECONDS` | no | `300` | Maximum lifetime for token-scoped cached table providers; set to `0` to disable |
| `HARBORSQL_TABLE_CACHE_MAX_ENTRIES` | no | `1024` | Maximum token/table/region cache entries; set to `0` to disable |
| `HARBORSQL_UNSAFE_LOG_SQL` | no | `false` | Include redacted SQL text in internal tracing spans for controlled debugging; SQL is omitted from logs by default |
| `DATABRICKS_TOKEN` | query mode only | none | Token used by `harborsql query --sql ...` |

## Result Type Support

HarborSQL encodes Databricks SQL connector result pages directly from Arrow
arrays. The current Thrift result type matrix is explicit:

| Arrow/DataFusion type | Thrift result representation |
| --- | --- |
| `Boolean` | boolean |
| `Int8`, `Int16`, `Int32` | int |
| `Int64`, `UInt8`, `UInt16`, `UInt32` | bigint |
| `UInt64` | bigint only when the value fits in signed `i64` |
| `Float32`, `Float64` | double |
| `Utf8`, `LargeUtf8` | string |
| `Date32`, `Date64` | date metadata with string values |
| `Timestamp` | timestamp metadata with string values |

Other Arrow types, including decimal, binary, nested, interval, dictionary, and
time-only values, return `UNSUPPORTED_RESULT_TYPE` instead of being coerced.

## Observability

HarborSQL emits structured `tracing` spans for HTTP requests, Thrift RPCs,
query execution, Unity Catalog calls, Delta table opens, DataFusion planning and
execution, result materialization, fetches, and operation cancellation. HTTP
responses include an `x-request-id`; callers can provide one or let HarborSQL
generate it.

Prometheus-format metrics are available at `/metrics`. Expose `/metrics` only
on a trusted network or through an authenticated monitoring proxy.

SQL text is not logged by default. Query spans include a stable SQL hash and
length; set `HARBORSQL_UNSAFE_LOG_SQL=true` only in controlled debugging
environments to include centrally redacted SQL text.

## Development

Run the core checks:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

Connector smoke-test setup lives in [docs/ci-smoke-tests.md](docs/ci-smoke-tests.md).
Automation and coding-agent notes live in [AGENTS.md](AGENTS.md).

## Project Docs

- [Connector smoke tests](docs/ci-smoke-tests.md)
- [Release publishing](docs/release.md)
- [Benchmark policy](docs/benchmarks.md)
- [Security policy](SECURITY.md)

## Releases

Publishing a GitHub release runs the release workflow and publishes:

- `ghcr.io/<owner>/harborsql:<tag>` as a Linux x86_64 Docker image
- `ghcr.io/<owner>/harborsql-binaries:<tag>` as an OCI package containing binary archives
- the same binary archives as GitHub release assets

Release workflow details are in [docs/release.md](docs/release.md).

## Security

Security reporting and scope are documented in [SECURITY.md](SECURITY.md).

Operationally:

- Do not log or persist bearer tokens.
- Do not log or persist temporary cloud credentials.
- Keep `HARBORSQL_DATABRICKS_HOST` on HTTPS for real Databricks workspaces.
- Treat Unity Catalog as the authorization source of truth.
