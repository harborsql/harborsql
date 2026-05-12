# `REGEXP_REPLACE` Line-Break Compatibility Finding

## Summary

ClickBench Q29 exposed a deterministic result mismatch between Databricks SQL
Warehouse and HarborSQL/DataFusion when `REGEXP_REPLACE` is applied to `Referer`
values that contain embedded line breaks.

The query extracts a grouping key with:

```sql
REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '$1')
```

Rows whose `Referer` starts with a domain such as `http://svpressa.ru/` but later
contains `LF` or `CRLF` are grouped under that domain by Databricks SQL Warehouse
on the benchmark table. DataFusion's native `regexp_replace`, and HarborSQL's
single-capture fast path, leave those values unchanged because unflagged `.`
does not match `\n` in Rust/DataFusion regex semantics.

This is a real validation failure, not floating-point drift: `COUNT(*)` changes
because rows are assigned to different grouping keys.

## Evidence

Diagnostic query shape:

```sql
WITH keyed AS (
  SELECT
    Referer,
    REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '$1') AS k,
    length(Referer) AS len
  FROM hits
  WHERE Referer <> ''
)
SELECT
  k,
  COUNT(*) AS c,
  SUM(len) AS len_sum,
  AVG(len) AS l,
  MIN(Referer) AS min_referer
FROM keyed
GROUP BY k
HAVING COUNT(*) > 100000
ORDER BY k;
```

Observed for the `svpressa.ru` key on the same ClickBench table:

| Engine | `c` | `len_sum` |
| --- | ---: | ---: |
| Databricks SQL Warehouse | `242526` | `68834604` |
| HarborSQL/DataFusion | `242465` | `68794203` |

The delta for `svpressa.ru` is `61` rows. A targeted drilldown found `50`
distinct `Referer` strings representing those `61` rows. Each contains an
embedded line break. Databricks groups them under `svpressa.ru`; DataFusion uses
the original multi-line `Referer` as the key because the pattern does not match.

Disabling HarborSQL's custom `harborsql_regexp_replace_capture` fast path did not
change the result: native DataFusion `regexp_replace` produced the same mismatch.
That rules out HarborSQL's fast path as the root cause.

## Exact compatibility gap

The difference is the unflagged dot in the suffix:

```regex
.*$
```

DataFusion uses Rust regex behavior where `.` does not match `\n` unless DOTALL
mode is enabled. Databricks SQL Warehouse's table-column execution for this
benchmark groups the embedded-line-break values as if the tail can cross line
breaks.

A minimal DataFusion-compatible way to express the Databricks benchmark-column
behavior is to enable DOTALL explicitly:

```sql
REGEXP_REPLACE(Referer, '(?s)^https?://(?:www\.)?([^/]+)/.*$', '$1')
```

or to avoid relying on dot semantics:

```sql
REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/[\s\S]*$', '$1')
```

Prefer the `(?s)` form for readability and performance.

## Workaround

For SQL that is allowed to diverge from the original benchmark text, make DOTALL
explicit in the pattern:

```sql
REGEXP_REPLACE(Referer, '(?s)^https?://(?:www\.)?([^/]+)/.*$', '$1')
```

This keeps the key extraction behavior stable for line-break-containing URLs
without changing the `length(Referer)` input used by Q29's aggregate.

There is no HarborSQL or DataFusion runtime configuration setting that turns
DOTALL on globally for `REGEXP_REPLACE`. DOTALL is a regex-pattern option, so
users should opt in by rewriting the query pattern when this behavior is desired.
HarborSQL intentionally does not rewrite user regexes automatically because
applying DOTALL broadly can change valid queries where newlines are meant to be
boundaries.

## Diagnostic artifact

The benchmark repository contains a reproducible diagnostic SQL file:

```text
datasets/clickbench/diagnostics/q029_regexp_replace.sql
```

Use it to compare grouped-key output and targeted mismatch probes without
rerunning the full benchmark suite.
