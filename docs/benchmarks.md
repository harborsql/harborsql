# Benchmarks

Benchmark harnesses, topology notes, Unity Catalog setup runbooks, generated
results, server logs, and environment-specific datasets live outside this
engine repository.

Keep this repository focused on the HarborSQL runtime and public compatibility
fixtures. In particular, do not commit:

- generated benchmark result files
- raw server or client logs
- private workspace hostnames
- cloud account identifiers
- bucket names or object paths
- temporary credentials or presigned URLs

The release workflow can run a benchmark gate before publishing. For manual
release runs, override `benchmark_command` when the benchmark suite lives in a
separate checkout or needs a custom command. The default benchmark gate is the
repository-local release test command:

```bash
cargo test --release --locked --all-targets
```

If public benchmark summaries are added later, keep them aggregated and scrubbed
so they do not reveal private workspace, storage, credential, or network
topology details.

Compatibility findings that explain benchmark correctness differences can live
in this repository when they describe HarborSQL/DataFusion behavior rather than
private benchmark infrastructure. See
[`regexp-replace-linebreak-compatibility.md`](regexp-replace-linebreak-compatibility.md)
for the ClickBench Q29 `REGEXP_REPLACE` line-break finding and workaround
options.
