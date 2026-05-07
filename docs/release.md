# Release Publishing

Publishing a GitHub release runs the release workflow and publishes:

- `ghcr.io/<owner>/harborsql:<tag>` as a Linux x86_64 Docker image
- `ghcr.io/<owner>/harborsql-binaries:<tag>` as an OCI package containing the
  Linux x86_64 binary archive, plus the macOS Apple Silicon archive when
  enabled
- the same binary archives as GitHub release assets

For non-prerelease GitHub releases, the macOS artifact is mandatory and the
workflow also updates the `latest` tags. GitHub prereleases skip the macOS
artifact by default; add `[build-macos]` to the prerelease notes to include it.

The Docker image is built from the already-compiled Linux binary, so the
release build does not compile the Rust code again inside Docker.

```bash
docker pull ghcr.io/<owner>/harborsql:<tag>
oras pull ghcr.io/<owner>/harborsql-binaries:<tag>
```

To run the release validation without publishing, use the `Release` workflow's
manual `workflow_dispatch` trigger with `publish` disabled. The manual trigger
also has a `build_macos` option when a prerelease candidate needs the macOS
binary.

The default pre-release benchmark gate is:

```bash
cargo test --release --locked --all-targets
```

Override `benchmark_command` in the manual workflow run if the benchmark suite
lives in another repository or needs a different command.

GitHub Packages may create the first GHCR package as private. If this
repository is public and the images should be public, change the package
visibility in the GitHub package settings after the first publish.

The release workflow does not require Databricks secrets. Publishing to GHCR and
uploading release assets use the built-in `GITHUB_TOKEN`.
