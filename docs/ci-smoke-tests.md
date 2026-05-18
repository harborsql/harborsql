# Connector Smoke Tests

HarborSQL includes ignored integration tests for Databricks SQL connector
compatibility. The default `cargo test` run does not execute these tests unless
they are explicitly selected with `--ignored`.

## Local Connector Smoke

The local smoke test runs the Python Databricks SQL connector against a local
HarborSQL process and uses a synthetic bearer token:

```bash
HARBORSQL_CONNECTOR_SMOKE_PYTHON=/path/to/python \
HARBORSQL_CONNECTOR_SMOKE_AUTH=local \
  cargo test --locked --test databricks_connector_smoke \
    python_databricks_sql_connector_can_execute_noop_statement -- --ignored
```

The test harness configures the connector for the local HTTP endpoint. This is
for local protocol testing only; production deployments should serve HarborSQL
over HTTPS or behind a TLS-terminating proxy.

## Databricks-Backed Smoke

The Databricks-backed smoke test validates the real Unity Catalog and Delta
read path:

- Databricks SQL connector authentication
- Unity Catalog table lookup
- temporary table credential vending
- Delta table open
- typed result metadata
- typed row fetches
- `fetchmany(1)` pagination behavior

Run with a PAT:

```bash
HARBORSQL_CONNECTOR_SMOKE_AUTH=pat \
DATABRICKS_TOKEN=<token> \
HARBORSQL_DATABRICKS_HOST=https://<workspace-host> \
HARBORSQL_AWS_REGION=<aws-region> \
  cargo test --locked --test databricks_connector_smoke \
    python_databricks_sql_connector_can_execute_type_matrix_probe_query -- --ignored
```

Run with OAuth machine-to-machine credentials by setting:

- `DATABRICKS_HOST` or `HARBORSQL_DATABRICKS_HOST`
- `DATABRICKS_ACCOUNT_ID` when required by the workspace
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`
- `HARBORSQL_CONNECTOR_SMOKE_AUTH=oauth`

Set `HARBORSQL_CONNECTOR_SMOKE_TYPE_MATRIX_TABLE` or
`HARBORSQL_CONNECTOR_SMOKE_TYPE_MATRIX_QUERY` to point at the private probe
table for the environment under test.

## Required Unity Catalog Grants

The service principal or user behind the smoke-test credential must be able to
resolve the table, vend temporary credentials, and read it:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<principal>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<principal>`;
GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<principal>`;
GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `<principal>`;
```

The configured `HARBORSQL_AWS_REGION` must match the object storage region for
the table. A wrong region can surface as S3 redirect or object-store errors
when opening the Delta log.

## GitHub Actions

The GitHub Actions workflow always runs the local connector smoke test.
Databricks-backed integration smoke is split into the `databricks-smoke`
environment, runs only on pushes to `main`, and is opt-in with
`DATABRICKS_CONNECTOR_SMOKE_ENABLED=true`. When enabled, the workflow runs
separate PAT and OAuth matrix legs with fail-fast disabled so both auth modes
report a result even if one leg fails.

OAuth-backed integration smoke uses these `databricks-smoke` environment
secrets:

- `BENCH_US_DATABRICKS_HOSTNAME`
- `BENCH_EU_DATABRICKS_HOSTNAME`
- `DATABRICKS_ACCOUNT_ID`
- `TEST_CI_DATABRICKS_CLIENT_ID`
- `TEST_CI_DATABRICKS_CLIENT_SECRET`

PAT-backed integration smoke uses these `databricks-smoke` environment
secrets. The PAT must belong to the configured workspace and must be able to
read the type-matrix table through Unity Catalog:

- `TEST_CI_DATABRICKS_PAT`
- `TEST_CI_DATABRICKS_PAT_HOSTNAME`

Delete repository-level copies of these secrets after adding the environment
secrets so only the Databricks smoke job can request them.
