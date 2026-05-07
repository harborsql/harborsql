# Agent Notes

These notes are for automated coding agents and maintainers working in this
repository. Keep the public README focused on what HarborSQL is, how to run it,
and where to find stable user-facing docs.

## Repository Hygiene

- Do not commit generated benchmark results, local logs, `.env` files, private
  Databricks workspace identifiers, cloud storage paths, or temporary
  credentials.
- Keep environment-specific runbooks outside this repository unless the content
  is intentionally public and scrubbed.
- Do not add concrete bearer tokens, PATs, OAuth client secrets, AWS keys,
  presigned URLs, or temporary object-store credentials to tests or docs.
- Prefer placeholders such as `<workspace-host>`, `<catalog>`, `<schema>`,
  `<table>`, and `<service-principal-application-id>` in public docs.
- If a workflow run created while the repository is private contains operational
  details, delete it before making the repository public.

## Common Checks

Run focused checks after documentation-only changes:

```bash
cargo metadata --locked --format-version=1 --no-deps >/dev/null
git diff --check
```

Run the full Rust checks after source or workflow changes:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```

## Documentation Boundaries

- User-facing overview and quick start: `README.md`
- Vulnerability reporting and security scope: `SECURITY.md`
- Connector smoke-test and CI setup: `docs/ci-smoke-tests.md`
- Release workflow details: `docs/release.md`
- Benchmark repository policy: `docs/benchmarks.md`

## GitHub Actions

The default workflow permissions should remain read-only unless a workflow
requires publishing. Release jobs that upload assets or packages should request
write permissions only in the specific publishing jobs.

Databricks-backed smoke tests are opt-in and require repository secrets. Keep
fork pull requests safe by ensuring those tests skip cleanly when secrets are
not available.

## Databricks Profiles

When using local Databricks CLI profiles, unset `DATABRICKS_HOST` and
`DATABRICKS_TOKEN` so environment variables do not override the selected
profile:

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN databricks <command> --profile <profile>
```
