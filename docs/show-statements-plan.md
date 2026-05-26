# Databricks SHOW Statement Support Plan

Date: 2026-05-25

## Goal

Add a metadata execution path for the Databricks `SHOW` statements needed by
connectors and interactive clients without sending these statements through the
normal DataFusion query planner.

The metadata path must forward the caller's received Databricks credentials to
Unity Catalog on every upstream metadata request. It must not request temporary
table credentials or open Delta tables unless a later metadata feature truly
requires table data access.

## Initial Syntax Scope

Support this Databricks SQL subset first:

```sql
SHOW SCHEMAS [ { FROM | IN } catalog_name ] [ [ LIKE ] regex_pattern ]
SHOW CATALOGS [ [ LIKE ] regex_pattern ]
SHOW TABLES [ { FROM | IN } schema_name ] [ [ LIKE ] regex_pattern ]
SHOW VIEWS [ { FROM | IN } schema_name ] [ [ LIKE ] regex_pattern ]
SHOW TABLE EXTENDED [ { IN | FROM } schema_name ] LIKE regex_pattern
    [ PARTITION clause ]
```

Out of scope for this phase:

- `SHOW COLUMNS`
- `SHOW FUNCTIONS`
- `SHOW GRANTS`
- `SHOW TBLPROPERTIES`
- `SHOW CREATE`
- `SHOW PARTITIONS`
- partition-aware `SHOW TABLE EXTENDED` details

## Architecture

Introduce a small metadata subsystem in front of DataFusion:

```text
src/engine/metadata/
  mod.rs          # entrypoint for parsing and executing metadata statements
  parser.rs       # Databricks SHOW syntax to MetadataStatement
  pattern.rs      # shared pattern matching for SHOW filters
  source.rs       # MetadataSource trait used by the engine
  unity.rs        # Unity Catalog REST-backed MetadataSource
  result.rs       # Databricks-compatible QueryResult builders
```

The engine should try this path before read-only SQL validation:

```rust
if let Some(statement) = metadata::parse_show_statement(sql)? {
    return metadata::execute(
        self.metadata_source.as_ref(),
        bearer_token,
        statement,
        default_catalog,
        default_schema,
    )
    .await;
}
```

Use an enum as the internal contract:

```rust
enum MetadataStatement {
    ShowCatalogs {
        pattern: Option<String>,
    },
    ShowSchemas {
        catalog: Option<ObjectName>,
        pattern: Option<String>,
    },
    ShowTables {
        schema: Option<ObjectName>,
        pattern: Option<String>,
    },
    ShowViews {
        schema: Option<ObjectName>,
        pattern: Option<String>,
    },
    ShowTableExtended {
        schema: Option<ObjectName>,
        pattern: String,
        partition: Option<PartitionSpec>,
    },
}
```

Add a `MetadataSource` trait so tests can verify credential propagation without
real Databricks calls:

```rust
#[async_trait]
trait MetadataSource: Send + Sync {
    async fn list_catalogs(&self, bearer_token: &str) -> Result<Vec<CatalogInfo>>;
    async fn list_schemas(
        &self,
        bearer_token: &str,
        catalog_name: &str,
    ) -> Result<Vec<SchemaInfo>>;
    async fn list_tables(
        &self,
        bearer_token: &str,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<TableInfo>>;
    async fn table(&self, bearer_token: &str, full_name: &str) -> Result<TableInfo>;
}
```

The existing Unity Catalog table lookup can be reused for `table`, but list
operations should be added to `UnityCatalogClient` and wired through this trait.

## Namespace Rules

Resolve object names before calling Unity Catalog:

- `SHOW CATALOGS` has no namespace input.
- `SHOW SCHEMAS` uses the default/session catalog unless `IN catalog_name` or
  `FROM catalog_name` is provided.
- `SHOW TABLES`, `SHOW VIEWS`, and `SHOW TABLE EXTENDED` use the default/session
  catalog and schema when no namespace is provided.
- An unqualified schema name resolves as `<default_catalog>.<schema>`.
- A two-part schema name resolves as `<catalog>.<schema>`.
- Quoted identifiers must preserve case and special characters.

## Result Shapes

Return successful empty result sets with the correct schema when Unity returns no
matching rows.

Expected output columns:

| Statement | Columns |
| --- | --- |
| `SHOW CATALOGS` | `catalog` string |
| `SHOW SCHEMAS` | `databaseName` string |
| `SHOW TABLES` | `database` string, `tableName` string, `isTemporary` boolean |
| `SHOW VIEWS` | `namespace` string, `viewName` string, `isTemporary` boolean |
| `SHOW TABLE EXTENDED` | `database` string, `tableName` string, `isTemporary` boolean, `information` string |

`isTemporary` should be `false` until HarborSQL has session-local temporary
objects.

## Test Plan

### Parser Tests

- Parse `SHOW CATALOGS`.
- Parse `SHOW CATALOGS LIKE 'main*'`.
- Parse `SHOW CATALOGS 'main*'`.
- Parse `SHOW SCHEMAS`.
- Parse `SHOW SCHEMAS FROM main`.
- Parse `SHOW SCHEMAS IN main`.
- Parse `SHOW SCHEMAS LIKE 'sales*'`.
- Parse `SHOW SCHEMAS IN main LIKE 'sales*'`.
- Parse `SHOW TABLES`.
- Parse `SHOW TABLES FROM sales`.
- Parse `SHOW TABLES IN main.sales`.
- Parse `SHOW TABLES LIKE 'fact*'`.
- Parse `SHOW VIEWS`.
- Parse `SHOW VIEWS FROM sales`.
- Parse `SHOW VIEWS IN main.sales`.
- Parse `SHOW VIEWS LIKE 'dim*'`.
- Parse `SHOW TABLE EXTENDED IN sales LIKE 'fact*'`.
- Parse `SHOW TABLE EXTENDED FROM main.sales LIKE 'fact*' PARTITION (dt='2026-05-25')`.
- Parse case, whitespace, multiline, and semicolon variants.
- Reject `SHOW SCHEMAS HISTORY`.
- Reject malformed partitions.
- Reject `SHOW TABLE EXTENDED` without a required `LIKE` pattern.
- Reject unsupported `SHOW` variants with `UNSUPPORTED_SQL`.

### Namespace Resolution Tests

- `SHOW SCHEMAS` calls `list_schemas` with the default catalog.
- `SHOW SCHEMAS IN catalog_a` calls `list_schemas` with `catalog_a`.
- `SHOW TABLES` calls `list_tables` with the default catalog and schema.
- `SHOW TABLES IN schema_a` resolves to `<default_catalog>.schema_a`.
- `SHOW TABLES IN catalog_a.schema_a` resolves to `catalog_a.schema_a`.
- `SHOW VIEWS` follows the same rules as `SHOW TABLES`.
- `SHOW TABLE EXTENDED` follows the same rules as `SHOW TABLES`.
- Quoted identifiers preserve exact spelling in Unity Catalog calls.

### Credential Propagation Tests

- A mock metadata source records the bearer token for `SHOW CATALOGS`.
- A mock metadata source records the bearer token for `SHOW SCHEMAS`.
- A mock metadata source records the bearer token for `SHOW TABLES`.
- A mock metadata source records the bearer token for `SHOW VIEWS`.
- A mock metadata source records the bearer token for every `table` call made by
  `SHOW TABLE EXTENDED`.
- HTTP `Authorization: Bearer token-a` is forwarded as `token-a`.
- Databricks Basic auth with username `token` forwards the decoded PAT.
- Thrift `ExecuteStatement("SHOW SCHEMAS")` uses the bearer token received by
  the Thrift request.
- Two callers issuing the same `SHOW TABLES` keep distinct tokens.
- Missing or invalid auth never calls the metadata source.

### Unity API Client Tests

- `list_catalogs` sends `Authorization: Bearer <token>`.
- `list_schemas(catalog)` sends the bearer token and the expected catalog query
  parameter.
- `list_tables(catalog, schema)` sends the bearer token and expected namespace
  query parameters.
- `table(full_name)` sends the bearer token and URL-encodes the full name.
- Paginated list calls follow `next_page_token`.
- Every paginated request preserves the same bearer token.
- Non-200 responses become `HarborError::Unity`.
- Error messages are redacted and truncated.
- Query parameters are encoded for spaces, dots, and special characters.

### Result Shape Tests

- `SHOW CATALOGS` returns exactly the `catalog` column.
- `SHOW SCHEMAS` returns exactly the `databaseName` column.
- `SHOW TABLES` returns `database`, `tableName`, and `isTemporary`.
- `SHOW VIEWS` returns `namespace`, `viewName`, and `isTemporary`.
- `SHOW TABLE EXTENDED` returns `database`, `tableName`, `isTemporary`, and
  `information`.
- Empty Unity results return zero rows with the correct columns.
- Result rows are stable-sorted when Unity does not guarantee order.
- `isTemporary` is `false` for all rows in this phase.

### Execution Boundary Tests

- Supported `SHOW` statements bypass DataFusion planning.
- Supported `SHOW` statements do not call `temporary_table_credentials`.
- Supported `SHOW` statements do not call the Delta table opener.
- Existing `SELECT` planning and execution remains unchanged.
- Unsupported metadata syntax returns `UNSUPPORTED_SQL`, not `DATAFUSION_ERROR`.
- Unity failures propagate as `UNITY_CATALOG_ERROR` to clients.

### Pattern Tests

- `*` matches any substring.
- `|` matches alternatives.
- Matching is case-insensitive.
- Patterns are trimmed before matching.
- Literal dots and underscores behave according to the Databricks pattern rules
  selected for this implementation.
- Empty patterns are rejected or treated consistently with Databricks semantics.
- Invalid patterns return `UNSUPPORTED_SQL` instead of panicking.

### Table and View Semantics Tests

- `SHOW TABLES` includes table-like Unity objects and excludes views.
- `SHOW VIEWS` includes views and excludes table-like objects.
- Unknown or inaccessible schemas surface Unity Catalog behavior consistently.
- Table names from Unity Catalog are filtered after namespace resolution.
- View names from Unity Catalog are filtered after namespace resolution.

### SHOW TABLE EXTENDED Tests

- The command first lists matching tables in the resolved namespace.
- For each matched table, `table(full_name)` is called with the received bearer
  token.
- Multiple matched tables call `table(full_name)` once per table.
- Views are excluded.
- No matched tables returns an empty result.
- Partition syntax is parsed into the internal statement representation.
- Partition-specific execution returns `UNSUPPORTED_SQL` until implemented.
- The `information` field is deterministic and redacts storage locations if
  they are included in future output.

### High-Value Regression Test

Add one end-to-end engine test for:

```sql
SHOW SCHEMAS
```

Assertions:

- the metadata source receives the exact bearer token passed to
  `QueryEngine::execute`;
- `list_schemas` is called with the default catalog;
- no table credentials are requested;
- the Delta table opener is not called;
- the result schema is `databaseName`;
- the result rows match the mocked Unity schemas.

This test proves the new path is credential-forwarding, metadata-only, and
Databricks-compatible at the result boundary.

## Implementation Phases

1. Add the parser, namespace resolver, pattern matcher, and mocked metadata
   executor.
2. Add result builders and engine integration before DataFusion planning.
3. Add Unity Catalog list APIs and pagination.
4. Wire HTTP and Thrift paths through the same engine path.
5. Add `SHOW TABLE EXTENDED` basic metadata output.
6. Leave partition-specific extended output explicitly unsupported until Delta
   partition metadata is designed.
