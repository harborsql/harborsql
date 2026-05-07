# Security Policy

## Supported Versions

Security fixes target `main` and the latest tagged release. Pre-release builds
are supported only when they are the most recent published artifact.

## Reporting a Vulnerability

Please do not report suspected vulnerabilities in public issues, pull requests,
or discussions.

Use GitHub private vulnerability reporting or a private GitHub security advisory
for this repository. If private reporting is unavailable, open a public issue
asking for a security contact and do not include technical details until a
private channel is available.

Include enough information for maintainers to reproduce and assess the issue:

- affected version, commit, or container tag
- deployment mode and relevant configuration, with secrets removed
- a minimal proof of concept or request/response sequence
- expected impact and any known exploitation constraints
- logs only after tokens, temporary cloud credentials, object paths, URLs, and
  SQL text have been redacted

We aim to acknowledge reports within 3 business days and provide an initial
assessment within 7 business days. Fix and disclosure timelines depend on
severity, exploitability, and whether credential rotation or coordinated
dependency updates are required.

## Scope

Reports are especially useful for issues in these areas:

- Databricks bearer token, OAuth, or PAT handling
- Unity Catalog authorization or credential vending boundaries
- temporary AWS/object-store credential exposure
- object-store routing across tables, prefixes, or tenants
- SQL validation bypasses that permit unsafe statements
- Thrift protocol parsing, container limits, or denial-of-service behavior
- client-visible error messages that expose operational details
- dependency vulnerabilities with a practical HarborSQL impact

Issues in private benchmark infrastructure, external Databricks workspace
configuration, or cloud account policy are out of scope unless they demonstrate
a vulnerability in HarborSQL itself.
