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
- Databricks-style result metadata and inline result encoding for scalar,
  decimal, binary, and nested Arrow result columns
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

HarborSQL assumes you already have a working Databricks workspace with Unity
Catalog Delta tables and users or service principals that can read those
tables.

To migrate that workload to HarborSQL, you need:

- A HarborSQL runtime:
  - Docker image: [`ghcr.io/harborsql/harborsql:<tag>`](https://github.com/orgs/harborsql/packages/container/package/harborsql)
  - Binary archives from [GitHub Releases](https://github.com/harborsql/harborsql/releases) or [`ghcr.io/harborsql/harborsql-binaries:<tag>`](https://github.com/orgs/harborsql/packages/container/package/harborsql-binaries)
- One extra Unity Catalog grant on each schema you want to query from HarborSQL:

```sql
GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<principal>`;
```

Your existing Unity Catalog read permissions still apply. HarborSQL does not
need static cloud credentials; Unity Catalog vends temporary table credentials
at query time. See the documentation site for the full Unity Catalog grant
model.

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
| `HARBORSQL_DATABRICKS_COUNT_STAR_ALIAS_REWRITE` | no | `true` | Alias unaliased `COUNT(*)` projections as `count(1)` to match Databricks SQL Warehouse column metadata; set to `false` to disable |
| `HARBORSQL_DATABRICKS_EXPRESSION_ALIAS_REWRITE` | no | `true` | Alias unaliased expression projections with Databricks-style names to avoid DataFusion-specific typed literal metadata; set to `false` to disable |
| `HARBORSQL_UNSAFE_LOG_SQL` | no | `false` | Include redacted SQL text in internal tracing spans for controlled debugging; SQL is omitted from logs by default |
| `DATABRICKS_TOKEN` | query mode only | none | Token used by `harborsql query --sql ...` |

## Result Type Support

HarborSQL encodes Databricks SQL connector result pages directly from Arrow
arrays. The current Thrift result type matrix is explicit:

| Arrow/DataFusion type | Databricks type metadata | Thrift value representation |
| --- | --- | --- |
| `Boolean` | boolean | boolean |
| `Int8` | tinyint | int |
| `Int16` | smallint | int |
| `Int32` | int | int |
| `Int64`, `UInt8`, `UInt16`, `UInt32` | bigint | bigint |
| `UInt64` | bigint | bigint only when the value fits in signed `i64` |
| `Float32` | float | double |
| `Float64` | double | double |
| `Utf8`, `LargeUtf8`, `Utf8View` | string | string |
| `Date32`, `Date64` | date | string |
| `Timestamp` | timestamp | string |
| `Binary`, `LargeBinary`, `FixedSizeBinary` | binary | binary |
| `Decimal128`, `Decimal256` | decimal with precision/scale qualifiers | string |
| `List`, `LargeList`, `FixedSizeList` | array | string |
| `Map` | map | string |
| `Struct` | struct | string |

Decimal and nested values are rendered in the compact Databricks-style display
form expected by connector compatibility tests. Arrays render as bracketed
values, maps and structs render as JSON-like objects, string values are quoted,
and nested date/timestamp values use Databricks-style textual dates and
timestamps.

Unsupported Arrow types, including interval, dictionary, duration, list views,
and time-only values, return `UNSUPPORTED_RESULT_TYPE` instead of being coerced.

## SQL Compatibility Notes

HarborSQL applies a small set of compatibility rewrites before handing SQL to
DataFusion. These rewrites keep common Databricks SQL connector and benchmark
queries working while leaving general SQL semantics to DataFusion.

- Unaliased `COUNT(*)` projections are aliased as `count(1)` by default.
- Unaliased expression projections are assigned Databricks-style metadata names
  by default.
- Simple contains-style `LIKE '%literal%'` predicates can be rewritten to
  DataFusion `contains(...)`.
- Single-capture `REGEXP_REPLACE(..., '$1')` shapes can be rewritten to a
  HarborSQL UDF.
- `extract(minute FROM timestamp)` is rewritten to a HarborSQL UDF for
  Databricks-compatible minute extraction.
- Databricks `get(array, zero_based_index)` is rewritten to DataFusion
  `array_element(...)` with one-based index adjustment and negative-index
  null behavior. `get(...).field` is rewritten to DataFusion named-field
  bracket access.

See [docs/delta-types-compatibility.md](docs/delta-types-compatibility.md) for
the decimal, binary, nested-result, and `get(array, index)` compatibility notes.

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
- [Delta types compatibility](docs/delta-types-compatibility.md)
- [REGEXP_REPLACE line-break compatibility finding](docs/regexp-replace-linebreak-compatibility.md)
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
