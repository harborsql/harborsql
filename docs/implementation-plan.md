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
- Use a dedicated Unity Catalog Delta probe table for the first smoke test.
  Keep concrete workspace/schema/table names in the benchmark/runbook repo.

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

Current status:

- Implemented a minimal Thrift-over-HTTP `TCLIService` endpoint on Databricks
  warehouse-style paths such as `/sql/1.0/warehouses/<id>`.
- Implemented `SET ...` as a no-op so benchmark clients can disable result
  caching before the real query.
- Implemented inline columnar result sets. This avoids Databricks Cloud Fetch
  and keeps the first compatibility slice local.
- Implemented query-history duration responses for completed local operations.
- Implemented the connector feature-flags endpoint with an empty flag set so
  local connector startup does not wait on Databricks-only service endpoints.

### Phase 3: Local Client Workaround

Because the stock Python connector forces HTTPS for normal hostname settings,
support local development in two ways:

- default server port: `1992`
- local-only client shim using connector `_connection_uri`
- optional TLS reverse proxy documentation for `localhost:443`

Production remains standard HTTPS on port 443.

Verified local Python connector shape:

```python
from databricks import sql

connection = sql.connect(
    server_hostname="http://127.0.0.1:1992",
    http_path="/sql/1.0/warehouses/<warehouse-id>",
    access_token=token,
    catalog="workspace",
    schema="<schema>",
    _connection_uri="http://127.0.0.1:1992/sql/1.0/warehouses/<warehouse-id>",
    use_cloud_fetch=False,
)
```

The `_connection_uri` override is required for local HTTP. Without it, the
connector constructs an HTTPS URL for the Thrift transport. In production,
HarborSQL should be served over HTTPS on port 443 so standard
`server_hostname`/`http_path` settings can be used.

#### Native HTTPS Proposal

Native HTTPS support is feasible because HarborSQL currently wraps all HTTP and
Thrift routes in one Axum app before binding a plain TCP listener. The first
implementation should be opt-in and should not change the default local HTTP
behavior:

- add `HARBORSQL_TLS_CERT_FILE` and `HARBORSQL_TLS_KEY_FILE` for serving the
  existing app over rustls with a user-provided certificate chain and key
- keep `HARBORSQL_BIND_ADDR` as the single bind address, with the log line
  reporting `http://` or `https://` according to the selected listener
- keep reverse proxies as the recommended production deployment path, because
  they handle public certificate issuance, renewal, redirects, and port 443
  ownership better than the HarborSQL process

For local development, a generated self-signed certificate is useful but should
be explicit and local-only. Self-signed TLS gives encryption, but clients still
need either a trusted CA/certificate file or an insecure verification override
to accept the server identity. This means self-signed HTTPS improves protocol
compatibility testing but does not make every Databricks client work with
standard production-style settings by itself.

A local self-signed mode should:

- require an explicit opt-in, for example `HARBORSQL_TLS_SELF_SIGNED_LOCAL=true`
- generate subject alternative names for `localhost`, `127.0.0.1`, and `::1`
- refuse to run when binding to non-loopback addresses such as `0.0.0.0`
- write the generated certificate and key outside the repository, with file
  permissions that avoid exposing the private key
- print the certificate path and client snippets for Python and JDBC, such as
  `_tls_trusted_ca_file=<cert.pem>` or JDBC trust-store/self-signed options
- prefer a locally trusted CA flow such as `mkcert` when the goal is
  no-extra-client-configuration local HTTPS

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
