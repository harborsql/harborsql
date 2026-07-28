# Memory Stability Test

HarborSQL has an opt-in integration test that starts the real server, sends one
query per second for one hour, and samples the server process's resident set
size (RSS) after every response. It fails on any query error, unexpected
response, early server exit, excessive ending memory growth, or excessive peak
RSS.

By default, the workload reads one row from
`bench_eu.harborsql_clickbench_s3.hits_optimized` in the Bench EU workspace.
This exercises Unity Catalog
authorization and temporary credential vending, Delta table loading, object
storage reads, HTTP handling, planning, execution, result materialization, and
teardown. The configured principal needs the grants described in
[ci-smoke-tests.md](ci-smoke-tests.md).

Set the Bench EU workspace host and a PAT before running the test. The token is
forwarded to Unity Catalog but is not passed to the HarborSQL child process:

```bash
export DATABRICKS_HOST="https://<bench-eu-workspace-host>"
export DATABRICKS_TOKEN="<token>"

cargo test --release --locked --test stability \
  low_rate_query_workload_has_stable_memory -- --ignored --nocapture
```

Use a release build so debug-only overhead does not distort the result.

The default test parameters are:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `HARBORSQL_STABILITY_DURATION_SECONDS` | `3600` | Total workload duration |
| `HARBORSQL_STABILITY_REQUEST_INTERVAL_MILLISECONDS` | `1000` | Target delay between request starts |
| `HARBORSQL_STABILITY_REQUEST_TIMEOUT_SECONDS` | `10` | Per-request HTTP timeout |
| `HARBORSQL_STABILITY_WARMUP_REQUESTS` | `30` | Initial RSS samples excluded from analysis |
| `HARBORSQL_STABILITY_COMPARISON_WINDOW` | `60` | Samples used for the starting and ending medians |
| `HARBORSQL_STABILITY_MAX_GROWTH_MIB` | `128` | Maximum ending-median RSS growth after warmup |
| `HARBORSQL_STABILITY_MAX_RSS_MIB` | `1024` | Maximum RSS sample after warmup |
| `HARBORSQL_STABILITY_DATABRICKS_HOST` | `BENCH_EU_DATABRICKS_HOSTNAME` or `DATABRICKS_HOST` | Bench EU workspace host |
| `HARBORSQL_STABILITY_DATABRICKS_TOKEN` | `DATABRICKS_TOKEN` or `TEST_CI_DATABRICKS_PAT` | PAT used for Unity Catalog requests |
| `HARBORSQL_STABILITY_AWS_REGION` | `HARBORSQL_AWS_REGION` or `eu-west-3` | Region for the Bench EU table's object storage |
| `HARBORSQL_STABILITY_SQL` | Bench EU `bench_eu.harborsql_clickbench_s3.hits_optimized` one-row query | Query to repeat; it must return exactly one row |

For a quick harness check, shorten the duration and warmup:

```bash
HARBORSQL_STABILITY_DURATION_SECONDS=15 \
HARBORSQL_STABILITY_REQUEST_INTERVAL_MILLISECONDS=100 \
HARBORSQL_STABILITY_WARMUP_REQUESTS=10 \
HARBORSQL_STABILITY_COMPARISON_WINDOW=20 \
cargo test --release --locked --test stability \
  low_rate_query_workload_has_stable_memory -- --ignored --nocapture
```

The summary reports query count and rate, starting and ending RSS medians,
ending growth, minimum and peak RSS, and the least-squares RSS trend per hour.
The test does not persist raw samples or logs. HarborSQL's temporary log is
included in a failure and removed when the test process exits.

RSS includes allocator-retained pages, so a cold process can grow before
settling even when it does not leak. The warmup exclusion and window medians
reduce that noise. Treat a threshold failure as an investigation signal:
repeat the test under comparable conditions and inspect the reported trend
before changing a limit.
