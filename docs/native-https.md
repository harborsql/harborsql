# Native HTTPS

Status: proposed.

HarborSQL should keep plain HTTP as the default local listener while adding an
opt-in native HTTPS path. The primary production recommendation remains a
TLS-terminating proxy, but native HTTPS would make local connector testing and
small deployments easier when a proxy is unnecessary.

## Motivation

Databricks clients work most naturally with HTTPS. The Python connector builds
an HTTPS Thrift transport for ordinary `server_hostname` and `http_path`
settings, and the current local HTTP workflow needs a connector-specific
`_connection_uri` override. JDBC can connect locally by disabling SSL, but that
also moves away from the production-style connection shape.

Native HTTPS support would let HarborSQL exercise the same HTTP routes and
Thrift protocol over TLS without requiring a separate reverse proxy in every
local environment.

## Recommended Shape

The first implementation should be opt-in and should not change the default
local HTTP behavior:

- add `HARBORSQL_TLS_CERT_FILE` and `HARBORSQL_TLS_KEY_FILE` for serving the
  existing app over rustls with a user-provided certificate chain and key
- keep `HARBORSQL_BIND_ADDR` as the single bind address, with the log line
  reporting `http://` or `https://` according to the selected listener
- keep reverse proxies as the recommended production deployment path, because
  they handle public certificate issuance, renewal, redirects, and port 443
  ownership better than the HarborSQL process

This is feasible because HarborSQL builds one Axum app for the health,
metrics, query, feature-flag, query-history, and Thrift routes before binding a
plain TCP listener. Native TLS can wrap that listener without changing route
behavior.

## Local Self-Signed Mode

For local development, a generated self-signed certificate is useful but should
be explicit and local-only. Self-signed TLS gives encryption, but clients still
need either a trusted CA/certificate file or an insecure verification override
to accept the server identity. This means self-signed HTTPS improves protocol
compatibility testing but does not make every Databricks client work with
standard production-style settings by itself.

A local self-signed mode should:

- require an explicit opt-in, for example `HARBORSQL_TLS_SELF_SIGNED_LOCAL=true`
- generate subject alternative names for `localhost`, `127.0.0.1`, and `::1`
- refuse to run when binding to non-loopback addresses such as `0.0.0.0`
- write the generated certificate and key outside the repository, with file
  permissions that avoid exposing the private key
- print the certificate path and client snippets for Python and JDBC, such as
  `_tls_trusted_ca_file=<cert.pem>` or JDBC trust-store/self-signed options
- prefer a locally trusted CA flow such as `mkcert` when the goal is
  no-extra-client-configuration local HTTPS

## Non-Goals

- Do not silently enable HTTPS by default.
- Do not install certificates into the operating-system trust store.
- Do not replace reverse proxies for public production deployments.
- Do not weaken TLS verification for real Databricks or HarborSQL endpoints.
