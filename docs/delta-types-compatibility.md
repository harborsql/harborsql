# Delta Types Compatibility

## Summary

The `delta-types` compatibility dataset exercises result types that are common
in Unity Catalog Delta tables but easy to miss in a narrow SQL engine surface:
decimals, binary values, arrays, maps, structs, and nested field access.

HarborSQL handles these cases in two places:

- Thrift result metadata and value encoding for Arrow result batches.
- Databricks SQL compatibility handling for `length(binary)` and
  `get(array, index)`.

No DataFusion bug is required for this behavior. DataFusion plans and executes
the supported Arrow expressions once HarborSQL exposes connector-compatible
result metadata and rewrites Databricks-specific syntax into DataFusion syntax.

## Result Metadata And Values

HarborSQL maps Arrow result types to Databricks `TCLIService` primitive type
IDs before returning inline columnar result pages:

| Arrow/DataFusion type | Databricks type metadata | Value encoding |
| --- | --- | --- |
| `Boolean` | boolean | boolean column |
| `Int8` | tinyint | int column |
| `Int16` | smallint | int column |
| `Int32` | int | int column |
| `Int64`, `UInt8`, `UInt16`, `UInt32` | bigint | bigint column |
| `UInt64` | bigint | bigint column only when the value fits in signed `i64` |
| `Float32` | float | double column |
| `Float64` | double | double column |
| `Utf8`, `LargeUtf8`, `Utf8View` | string | string column |
| `Date32`, `Date64` | date | string column |
| `Timestamp` | timestamp | string column |
| `Binary`, `LargeBinary`, `FixedSizeBinary` | binary | binary column |
| `Decimal128`, `Decimal256` | decimal with precision/scale qualifiers | string column |
| `List`, `LargeList`, `FixedSizeList`, list views | array | string column |
| `Map` | map | string column |
| `Struct` | struct | string column |

Decimals are returned as strings so clients see the scale-preserving decimal
display value. Nested arrays, maps, and structs are returned as compact
Databricks-style display strings. Strings inside nested values are quoted, nulls
render as `null`, dates render as `YYYY-MM-DD`, timestamps render as
`YYYY-MM-DD HH:MM:SS[.fraction]`, and map keys are emitted in stable sorted
order.

Unsupported Arrow types still fail explicitly with `UNSUPPORTED_RESULT_TYPE`.
That is intentional; HarborSQL should not silently coerce interval, duration,
dictionary, or time-only values into misleading connector results.

## `length(binary)`

Databricks `length(string)` returns a character count, while `length(binary)`
returns the number of bytes. DataFusion's built-in `length` is a string
function and may try to decode binary input as UTF-8. HarborSQL registers a
Databricks-compatible `length` implementation that preserves string character
counting and treats binary values as raw bytes.

## `get(array, index)`

Databricks `get(array, index)` uses a zero-based array index and returns `NULL`
for a negative index or an out-of-bounds index. DataFusion's `array_element`
function uses one-based positive indexes, treats negative indexes as offsets
from the end, and returns `NULL` for index `0`.

HarborSQL rewrites:

```sql
get(items, index)
```

to:

```sql
array_element(items, CASE WHEN index < 0 THEN 0 ELSE index + 1 END)
```

The rewrite preserves Databricks' zero-based positive index behavior and maps
negative indexes to DataFusion index `0`, which returns `NULL`. Out-of-bounds
positive indexes continue to return `NULL` through DataFusion.

For nested field access after `get`, HarborSQL also rewrites the computed struct
field access into DataFusion's named-field bracket form:

```sql
get(c_struct_all_complex.items, 0).prices
```

becomes:

```sql
array_element(
  c_struct_all_complex.items,
  CASE WHEN 0 < 0 THEN 0 ELSE 0 + 1 END
)['prices']
```

## Test Coverage

The local test suite covers the compatibility surface without requiring a live
Databricks workspace:

- `row_set_encodes_delta_type_compatibility_values` verifies decimal, binary,
  list, map, and struct value encoding.
- `metadata_response_encodes_decimal_and_complex_type_ids` verifies Databricks
  result metadata for decimal, array, map, struct, and binary columns.
- `rewrites_databricks_get_array_access` verifies the SQL rewrite shape.
- `execute_handles_delta_type_nested_field_access_query_shape` verifies the
  nested-access query shape plans and executes through DataFusion.

Run the coverage with:

```bash
cargo test row_set_encodes_delta_type_compatibility_values
cargo test metadata_response_encodes_decimal_and_complex_type_ids
cargo test rewrites_databricks_get_array_access
cargo test execute_handles_delta_type_nested_field_access_query_shape
cargo test
```
