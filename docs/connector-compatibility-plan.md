# Connector Compatibility Plan

Date: 2026-04-26

## Goal

HarborSQL should support Databricks client drivers without building one server
implementation per driver. The Python connector and JDBC driver should both sit
on top of the same connector core:

1. Parse the incoming driver protocol request.
2. Map it into a shared session, operation, metadata, and result model.
3. Execute SQL through the existing Unity Catalog + Delta + DataFusion engine.
4. Serialize the response in the protocol shape expected by the driver.

The compatibility goal is practical driver support, not full Databricks SQL
server parity.

## Driver Surfaces Reviewed

### Python Connector

The sibling `../databricks-sql-python` client uses a Thrift-over-HTTP
`TCLIService` path by default, with an optional Statement Execution API mode
when `use_sea=True`.

The common Thrift calls are:

- `OpenSession`
- `CloseSession`
- `ExecuteStatement`
- `GetOperationStatus`
- `GetResultSetMetadata`
- `FetchResults`
- `CloseOperation`
- `CancelOperation`
- metadata calls such as `GetCatalogs`, `GetSchemas`, `GetTables`, and
  `GetColumns`

The Python connector currently advertises protocol V7 in its `OpenSession`
request and accepts inline columnar results. That is the best first
compatibility target.

### JDBC Driver

The sibling `../databricks-jdbc` driver defaults to the Thrift client when
`UseThriftClient=1`, which is also the default. JDBC has a larger metadata
surface than the Python connector and requests protocol V9, but it gates
features based on the server protocol returned by `OpenSession`.

Important JDBC Thrift behavior:

- `OpenSession` sends session configuration and an optional initial namespace.
- `ExecuteStatement` may send `queryTimeout`, Arrow capability fields,
  Cloud Fetch capability fields, native parameters, and `resultRowLimit`.
- Direct results are controlled with `EnableDirectResults` and
  `RowsFetchedPerBlock`.
- Metadata calls include `GetTypeInfo`, `GetTableTypes`, `GetFunctions`,
  `GetPrimaryKeys`, and `GetCrossReference` in addition to the Python-used
  catalog/schema/table/column calls.
- JDBC can be kept on the Thrift path with `UseThriftClient=1`; SQL Exec API
  compatibility can be staged later.

## Protocol Strategy

Keep HarborSQL on the Thrift path first.

The current HarborSQL Thrift service already implements the statement lifecycle
needed for basic Python connector use. Extending that surface is less work and
less risk than introducing a second SQL Exec API implementation immediately.

Do not advertise a higher Thrift protocol version until the corresponding
features are implemented. In particular:

- V7 is acceptable for direct results, Cloud Fetch negotiation fields, and
  result persistence mode.
- V8 enables parameterized query assumptions in JDBC.
- V9 enables async metadata operation assumptions.

HarborSQL should keep returning V7 until native `TSparkParameter` parsing and
the broader metadata semantics are handled deliberately.

## Target Architecture

Split the current compatibility layer into a shared connector core and thin
protocol adapters.

```text
src/connector/
  operation_store.rs   # sessions, operations, cancellation, TTL, bearer-token scoping
  result.rs            # schema, row pages, limits, has_more_rows, row offsets
  metadata.rs          # catalogs, schemas, tables, columns, type info, table types
  capabilities.rs      # advertised protocol/API capabilities

src/thrift/
  codec.rs             # binary Thrift read/write helpers only
  service.rs           # TCLIService RPC mapping to connector core
  types.rs             # local typed request/response structs

src/sqlexec/
  routes.rs            # later: SQL Exec API JSON mapping to connector core
```

The shared connector core should not know whether the caller is Python, JDBC,
Thrift, or SQL Exec API JSON. It should expose operations such as:

- `open_session`
- `close_session`
- `execute_statement`
- `get_operation_status`
- `get_result_metadata`
- `fetch_results`
- `cancel_operation`
- `close_operation`
- `list_catalogs`
- `list_schemas`
- `list_tables`
- `list_columns`
- `list_type_info`
- `list_table_types`

## Work Plan

### Phase 1: Refactor Without Behavior Change

Extract the current session, operation, result paging, and query-history logic
from `src/thrift.rs` into a shared connector module.

Acceptance:

- Existing Python connector smoke path still works.
- Existing Rust tests keep passing.
- `src/thrift.rs` becomes mostly request decoding and response encoding.

### Phase 2: Add Metadata Core

Implement shared metadata responses for:

- catalogs
- schemas
- tables
- columns
- table types
- type info

Catalog/schema/table/column data should come from Unity Catalog where possible.
For unsupported metadata operations, return correct empty result sets with the
expected JDBC/Python column schema rather than protocol errors.

Acceptance:

- Python `cursor.catalogs()`, `schemas()`, `tables()`, and `columns()` work.
- JDBC `DatabaseMetaData.getCatalogs`, `getSchemas`, `getTables`, and
  `getColumns` work for common BI/tooling paths.

### Phase 3: Extend Thrift RPC Coverage

Add Thrift handlers for the JDBC metadata calls:

- `GetCatalogs`
- `GetSchemas`
- `GetTables`
- `GetColumns`
- `GetTypeInfo`
- `GetTableTypes`
- `GetFunctions`
- `GetPrimaryKeys`
- `GetCrossReference`

Initial support can return empty rows for functions and keys, as long as the
response schema and success status are correct.

Acceptance:

- JDBC connects with `UseThriftClient=1`.
- Basic `SELECT` through `java.sql.Statement` succeeds.
- Common metadata discovery calls do not fail the connection.

### Phase 4: Driver Compatibility Fixtures

Add golden and smoke tests from both drivers.

Recommended tests:

- capture representative Python Thrift request bytes for session, statement,
  status, metadata, and fetch calls
- capture representative JDBC Thrift request bytes for the same lifecycle
- assert HarborSQL decodes those requests into the same internal typed model
- assert response bytes are accepted by each driver in a local smoke test

Acceptance:

- Compatibility tests cover both Python and JDBC request shapes.
- Future protocol changes fail tests before breaking a driver.

### Phase 5: JDBC Statement Options

Implement the JDBC options that affect server behavior:

- `queryTimeout`
- `resultRowLimit`
- `RowsFetchedPerBlock` through `FetchResults.maxRows`
- direct-results behavior through `getDirectResults`
- `canReadArrowResult=false/true` negotiation, while still returning columnar
  inline results until Arrow is implemented
- `canDownloadResult=false` path for inline results

Native parameter support should be implemented before advertising protocol V8.

Acceptance:

- `Statement.setMaxRows` maps to result limiting.
- `Statement.setQueryTimeout` has a server-side effect.
- Fetch paging is stable with JDBC row block sizes.

### Phase 6: Optional SQL Exec API Adapter

Only after the Thrift path is stable, add SQL Exec API JSON endpoints and map
them to the same connector core:

- create session
- delete session
- execute statement
- get statement
- cancel statement
- close statement
- fetch chunk links or inline result data as needed

This should not duplicate the session, operation, metadata, or result paging
implementation.

## Local JDBC Smoke Shape

For local development, force JDBC onto the Thrift path and inline results:

```text
jdbc:databricks://127.0.0.1:1992/default;
transportMode=http;
ssl=0;
AuthMech=3;
UID=token;
PWD=<token>;
httpPath=/sql/1.0/warehouses/local;
UseThriftClient=1;
EnableQueryResultDownload=0
```

This should be treated as a compatibility test shape, not the final production
deployment shape. Production should still use HTTPS and normal Databricks-style
warehouse paths.

## Non-Goals For The First JDBC Slice

- Arrow result serialization.
- Cloud Fetch.
- SQL Exec API mode.
- Full prepared-statement/native-parameter parity.
- Write operations.
- Full Databricks SQL dialect support.
- Complete JDBC metadata semantics for keys, procedures, and functions.

## Main Risks

- The hand-written Thrift codec is already large. Expanding it without golden
  fixtures will make compatibility fragile.
- JDBC metadata is broader than Python metadata. Empty-but-correct result sets
  are safer than unsupported-method errors for the first slice.
- Advertising protocol V8/V9 too early will cause drivers to send requests that
  HarborSQL does not yet fully understand.
- JSON-backed result storage is convenient but should eventually be replaced by
  an Arrow/native row model to reduce lossy type conversion.

## Recommended Next Step

Start by extracting the connector core from `src/thrift.rs` without changing
behavior. Then add metadata RPCs on top of that shared core. This creates the
reuse boundary needed for both Python and JDBC and avoids duplicating protocol
logic later when SQL Exec API support is added.
