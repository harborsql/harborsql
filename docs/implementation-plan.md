# HarborSQL Implementation Plan

Date: 2026-04-25

## Direction

HarborSQL is an external SQL query engine for Unity Catalog Delta tables. It
should be compatible with Databricks SQL clients where practical, but it should
execute queries outside Databricks.

The production path is:

1. Databricks SQL connector sends a SQL request to HarborSQL.
2. HarborSQL extracts the client's bearer token.
3. HarborSQL calls Unity Catalog as that user.
4. HarborSQL resolves table metadata and vends temporary table credentials.
5. HarborSQL reads Delta data with `delta-rs`.
6. HarborSQL executes SQL with DataFusion.
7. HarborSQL returns Databricks-compatible result sets.

## MVP Scope

The first implementation is intentionally read-only and narrow:

- `SELECT` only.
- Delta tables only.
- AWS S3 credentials first.
- One Databricks workspace configured by `HARBORSQL_DATABRICKS_HOST`.
- Per-session client token forwarding; no persistent token storage.
- Use the known-good probe table for the first smoke test:
  `<catalog>.<schema>.<probe_table>`

## Phases

### Phase 1: Query Engine Core

Implement and test the hard path without Thrift:

- Unity Catalog REST client.
- Table metadata lookup.
- Temporary table credential vending.
- Delta table loading with `delta-rs`.
- SQL execution with DataFusion.
- Simple local HTTP JSON endpoint for smoke testing.

Acceptance:

```sql
SELECT * FROM <catalog>.<schema>.<probe_table>
SELECT COUNT(*) FROM <catalog>.<schema>.<probe_table>
```

### Phase 2: Databricks SQL Protocol Surface

Implement the minimal Databricks SQL connector surface:

- `OpenSession`
- `ExecuteStatement`
- `FetchResults`
- `GetOperationStatus`
- `CloseOperation`
- `CloseSession`
- `/api/2.0/sql/history/queries/{query_id}`

The protocol should initially return inline columnar/row results and can defer
Cloud Fetch.

### Phase 3: Local Client Workaround

Because the stock Python connector forces HTTPS for normal hostname settings,
support local development in two ways:

- default server port: `1992`
- local-only client shim using connector `_connection_uri`
- optional TLS reverse proxy documentation for `localhost:443`

Production remains standard HTTPS on port 443.

### Phase 4: SQL Coverage

Broaden read-only SQL support:

- projections
- filters
- aggregates
- ordering
- limits
- joins across multiple UC Delta tables
- catalog/schema defaults

DataFusion should provide most of this once table registration and name
resolution are correct.

## Non-Goals For MVP

- Writes.
- DDL.
- Views.
- Row filters and column masks.
- Databricks Cloud Fetch.
- Full Databricks SQL dialect compatibility.
- OAuth refresh for long-running queries.

## Initial Stack

- Rust
- Tokio
- Axum
- Reqwest
- DataFusion
- delta-rs
- Arrow JSON utilities for the first HTTP endpoint

## Security Rules

- Never log bearer tokens.
- Never persist bearer tokens or temporary cloud credentials.
- Redact credentials from errors and debug output.
- Scope temporary credentials to the operation requested from Unity Catalog.
- Treat all authorization as coming from Unity Catalog, not from local allow
  lists.
