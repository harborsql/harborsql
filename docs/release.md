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

Release builds restore Rust caches but do not save them. The separate
`Release Cache` workflow warms dependency caches from trusted `main` and
scheduled runs. Before compiling publishable artifacts, the release workflow
removes cached HarborSQL package outputs so dependencies stay cached while the
first-party binary is rebuilt from the checked-out release source.

Publishing jobs use the `release` GitHub environment and all workflow actions
are pinned to commit SHAs. GitHub release asset uploads do not use `--clobber`;
reruns fail instead of replacing existing release assets.

The release workflow scans the container image with Grype before pushing it,
publishes SPDX SBOMs for the image and native binary package, signs release
artifacts with keyless Sigstore/cosign, and signs the pushed image digest. For
public repositories, or private repositories with `ENABLE_GITHUB_ATTESTATIONS`
set after enabling GitHub artifact attestations for the plan, the workflow also
creates GitHub provenance and SBOM attestations.

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
