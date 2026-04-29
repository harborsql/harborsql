use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const DEFAULT_TYPE_MATRIX_TABLE: &str = "bench_eu.harborsql_delta_types.delta_type_matrix";

#[test]
#[ignore = "requires Python with databricks-sql-connector installed"]
fn python_databricks_sql_connector_can_execute_noop_statement() {
    run_connector_smoke(ConnectorSmoke::Noop);
}

#[test]
#[ignore = "requires Python with databricks-sql-connector installed and Databricks credentials"]
fn python_databricks_sql_connector_can_execute_type_matrix_probe_query() {
    if !databricks_backed_smoke_is_configured() {
        if require_databricks_smoke() {
            panic!("Databricks connector smoke test requires host plus PAT or OAuth credentials");
        }
        eprintln!("skipping Databricks-backed type matrix smoke test; credentials are not set");
        return;
    }

    run_connector_smoke(ConnectorSmoke::TypeMatrix);
}

#[derive(Debug, Clone, Copy)]
enum ConnectorSmoke {
    Noop,
    TypeMatrix,
}

impl ConnectorSmoke {
    fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::TypeMatrix => "type_matrix",
        }
    }
}

fn run_connector_smoke(smoke: ConnectorSmoke) {
    let bind_addr = unused_local_addr();
    let mut server = HarborSqlServer::spawn(bind_addr);
    wait_for_healthz(bind_addr, &mut server.child);
    let auth_mode = configured_auth_mode();

    let mut command = Command::new(python());
    command.arg("-c").arg(connector_smoke_script(bind_addr));
    command.env("HARBORSQL_CONNECTOR_SMOKE_KIND", smoke.as_str());
    if let Some(host) = configured_databricks_host() {
        command.env("DATABRICKS_HOST", host);
    }
    if let Some(client_id) = configured_client_id() {
        command.env("DATABRICKS_CLIENT_ID", client_id);
    }
    if let Some(client_secret) = configured_client_secret() {
        command.env("DATABRICKS_CLIENT_SECRET", client_secret);
    }
    if let Some(account_id) = optional_env("DATABRICKS_ACCOUNT_ID") {
        command.env("DATABRICKS_ACCOUNT_ID", account_id);
    }
    if auth_mode != "oauth"
        && let Some(token) = configured_pat_token()
    {
        command.env("DATABRICKS_TOKEN", token);
    }
    command.env("HARBORSQL_CONNECTOR_SMOKE_AUTH", auth_mode);
    command.env(
        "HARBORSQL_CONNECTOR_SMOKE_CATALOG",
        configured_connector_catalog(),
    );
    command.env(
        "HARBORSQL_CONNECTOR_SMOKE_SCHEMA",
        configured_connector_schema(),
    );
    if let Some(query) = optional_env("HARBORSQL_CONNECTOR_SMOKE_TYPE_MATRIX_QUERY") {
        command.env("HARBORSQL_CONNECTOR_SMOKE_TYPE_MATRIX_QUERY", query);
    }
    command.env(
        "HARBORSQL_CONNECTOR_SMOKE_TYPE_MATRIX_TABLE",
        configured_type_matrix_table(),
    );

    let output = command
        .output()
        .expect("failed to launch Python connector smoke test");

    assert!(
        output.status.success(),
        "Python connector smoke test failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

struct HarborSqlServer {
    child: Child,
}

impl HarborSqlServer {
    fn spawn(bind_addr: SocketAddr) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_harborsql"))
            .arg("server")
            .env("HARBORSQL_BIND_ADDR", bind_addr.to_string())
            .env("HARBORSQL_DATABRICKS_HOST", databricks_host())
            .env("HARBORSQL_DEFAULT_CATALOG", configured_connector_catalog())
            .env("HARBORSQL_DEFAULT_SCHEMA", configured_connector_schema())
            .env("RUST_LOG", "harborsql=debug")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to start HarborSQL server");

        Self { child }
    }
}

impl Drop for HarborSqlServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind local test port");
    listener
        .local_addr()
        .expect("failed to read local test port")
}

fn wait_for_healthz(addr: SocketAddr, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("failed to poll HarborSQL server") {
            panic!("HarborSQL server exited before health check succeeded: {status}");
        }

        if healthz_is_ready(addr) {
            return;
        }

        thread::sleep(Duration::from_millis(50));
    }

    panic!("HarborSQL server did not become healthy at http://{addr}/healthz");
}

fn healthz_is_ready(addr: SocketAddr) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let request = format!("GET /healthz HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }

    let mut response = String::new();
    stream.read_to_string(&mut response).is_ok()
        && (response.starts_with("HTTP/1.1 204") || response.starts_with("HTTP/1.0 204"))
}

fn python() -> String {
    std::env::var("HARBORSQL_CONNECTOR_SMOKE_PYTHON").unwrap_or_else(|_| "python3".to_string())
}

fn databricks_host() -> String {
    configured_databricks_host().unwrap_or_else(|| "https://example.com".to_string())
}

fn configured_databricks_host() -> Option<String> {
    optional_env("HARBORSQL_DATABRICKS_HOST")
        .or_else(|| optional_env("DATABRICKS_HOST"))
        .or_else(|| optional_env("BENCH_US_DATABRICKS_HOSTNAME"))
}

fn configured_client_id() -> Option<String> {
    optional_env("DATABRICKS_CLIENT_ID").or_else(|| optional_env("TEST_CI_DATABRICKS_CLIENT_ID"))
}

fn configured_client_secret() -> Option<String> {
    optional_env("DATABRICKS_CLIENT_SECRET")
        .or_else(|| optional_env("TEST_CI_DATABRICKS_CLIENT_SECRET"))
}

fn configured_pat_token() -> Option<String> {
    optional_env("DATABRICKS_TOKEN").or_else(|| optional_env("TEST_CI_DATABRICKS_PAT"))
}

fn configured_auth_mode() -> String {
    optional_env("HARBORSQL_CONNECTOR_SMOKE_AUTH").unwrap_or_else(|| "auto".to_string())
}

fn configured_connector_catalog() -> String {
    optional_env("HARBORSQL_CONNECTOR_SMOKE_CATALOG").unwrap_or_else(|| "workspace".to_string())
}

fn configured_connector_schema() -> String {
    optional_env("HARBORSQL_CONNECTOR_SMOKE_SCHEMA").unwrap_or_else(|| "default".to_string())
}

fn configured_type_matrix_table() -> String {
    optional_env("HARBORSQL_CONNECTOR_SMOKE_TYPE_MATRIX_TABLE")
        .unwrap_or_else(|| DEFAULT_TYPE_MATRIX_TABLE.to_string())
}

fn databricks_backed_smoke_is_configured() -> bool {
    let has_host = configured_databricks_host().is_some();
    let has_pat = configured_pat_token().is_some();
    let has_oauth = configured_client_id().is_some() && configured_client_secret().is_some();
    match configured_auth_mode().to_ascii_lowercase().as_str() {
        "local" => false,
        "pat" => has_host && has_pat,
        "oauth" => has_host && has_oauth,
        "auto" => has_host && (has_pat || has_oauth),
        _ => false,
    }
}

fn require_databricks_smoke() -> bool {
    optional_env("HARBORSQL_CONNECTOR_SMOKE_REQUIRE_DATABRICKS").is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn connector_smoke_script(addr: SocketAddr) -> String {
    format!(
        r#"
from databricks import sql
import datetime
import os

uri = "http://{addr}"
connection_uri = uri + "/sql/1.0/warehouses/local-smoke"
databricks_host = os.environ.get("DATABRICKS_HOST")
client_id = os.environ.get("DATABRICKS_CLIENT_ID")
client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
account_id = os.environ.get("DATABRICKS_ACCOUNT_ID")
pat_token = os.environ.get("DATABRICKS_TOKEN")
auth_mode = os.environ.get("HARBORSQL_CONNECTOR_SMOKE_AUTH", "auto").lower()
catalog = os.environ.get("HARBORSQL_CONNECTOR_SMOKE_CATALOG", "workspace")
schema = os.environ.get("HARBORSQL_CONNECTOR_SMOKE_SCHEMA", "default")
smoke_kind = os.environ.get("HARBORSQL_CONNECTOR_SMOKE_KIND", "noop").lower()

connect_kwargs = dict(
    server_hostname=uri,
    http_path="/sql/1.0/warehouses/local-smoke",
    catalog=catalog,
    schema=schema,
    _connection_uri=connection_uri,
    use_cloud_fetch=False,
)

if auth_mode not in ("auto", "local", "oauth", "pat"):
    raise AssertionError(f"unsupported smoke auth mode: {{auth_mode}}")

if auth_mode == "local":
    connect_kwargs["access_token"] = "local-token"
elif auth_mode == "pat":
    if not pat_token:
        raise AssertionError("PAT smoke test requires DATABRICKS_TOKEN or TEST_CI_DATABRICKS_PAT")
    connect_kwargs["access_token"] = pat_token
elif auth_mode == "oauth" or client_id or client_secret:
    missing = [
        name
        for name, value in [
            ("DATABRICKS_HOST", databricks_host),
            ("DATABRICKS_CLIENT_ID", client_id),
            ("DATABRICKS_CLIENT_SECRET", client_secret),
        ]
        if not value
    ]
    if missing:
        raise AssertionError(f"incomplete OAuth smoke test env: {{', '.join(missing)}}")

    from databricks.sdk.core import Config, oauth_service_principal

    if not databricks_host.startswith(("http://", "https://")):
        databricks_host = f"https://{{databricks_host}}"

    def credential_provider():
        config = Config(
            host=databricks_host,
            client_id=client_id,
            client_secret=client_secret,
            account_id=account_id,
        )
        return oauth_service_principal(config)

    connect_kwargs["credentials_provider"] = credential_provider
else:
    connect_kwargs["access_token"] = pat_token or "local-token"

connection = sql.connect(**connect_kwargs)

def require_query_id(cursor):
    query_id = getattr(cursor, "query_id", None)
    if not query_id:
        raise AssertionError("expected connector cursor to expose a query_id")

def run_noop(cursor):
    cursor.execute("SET use_cached_result = false")
    rows = cursor.fetchall()
    require_query_id(cursor)

    if rows != []:
        raise AssertionError(f"expected no rows, got {{rows!r}}")

def require_type(name, value, expected_type):
    if expected_type is int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise AssertionError(f"{{name}} expected int, got {{type(value).__name__}}: {{value!r}}")
    elif not isinstance(value, expected_type):
        raise AssertionError(f"{{name}} expected {{expected_type.__name__}}, got {{type(value).__name__}}: {{value!r}}")

def run_type_matrix_probe(cursor):
    table = os.environ.get(
        "HARBORSQL_CONNECTOR_SMOKE_TYPE_MATRIX_TABLE",
        "{default_type_matrix_table}",
    )
    query = os.environ.get("HARBORSQL_CONNECTOR_SMOKE_TYPE_MATRIX_QUERY")
    if not query:
        query = f"""
SELECT
  true AS bool_value,
  CAST(-8 AS TINYINT) AS tinyint_value,
  CAST(-16 AS SMALLINT) AS smallint_value,
  CAST(32 AS INT) AS int_value,
  CAST(64 AS BIGINT) AS bigint_value,
  CAST(1.25 AS FLOAT) AS float_value,
  CAST(2.5 AS DOUBLE) AS double_value,
  'harbor' AS string_value,
  DATE '2024-01-02' AS date_value,
  TIMESTAMP '2024-01-02 03:04:05' AS timestamp_value
FROM {{table}}
LIMIT 2
"""

    cursor.execute(query)
    first_page = cursor.fetchmany(1)
    remaining = cursor.fetchall()
    rows = list(first_page) + list(remaining)
    require_query_id(cursor)

    if not rows:
        raise AssertionError("type matrix probe query returned no rows")
    if len(first_page) > 1:
        raise AssertionError(f"fetchmany(1) returned too many rows: {{first_page!r}}")

    expected_rows = os.environ.get("HARBORSQL_CONNECTOR_SMOKE_EXPECTED_ROWS")
    if expected_rows and len(rows) != int(expected_rows):
        raise AssertionError(f"expected {{expected_rows}} rows, got {{len(rows)}}")

    require_pagination = os.environ.get(
        "HARBORSQL_CONNECTOR_SMOKE_REQUIRE_PAGINATION",
        "",
    ).lower() in ("1", "true", "yes", "on")
    if require_pagination and len(rows) < 2:
        raise AssertionError("pagination check requires the probe query to return at least two rows")

    description = cursor.description or []
    actual_names = [column[0] for column in description]
    expected_names = [
        "bool_value",
        "tinyint_value",
        "smallint_value",
        "int_value",
        "bigint_value",
        "float_value",
        "double_value",
        "string_value",
        "date_value",
        "timestamp_value",
    ]
    if actual_names != expected_names:
        raise AssertionError(f"unexpected columns: {{actual_names!r}}")

    row = rows[0]
    require_type("bool_value", row[0], bool)
    require_type("tinyint_value", row[1], int)
    require_type("smallint_value", row[2], int)
    require_type("int_value", row[3], int)
    require_type("bigint_value", row[4], int)
    require_type("float_value", row[5], float)
    require_type("double_value", row[6], float)
    require_type("string_value", row[7], str)
    if not isinstance(row[8], (datetime.date, str)):
        raise AssertionError(f"date_value expected date or str, got {{type(row[8]).__name__}}: {{row[8]!r}}")
    if not isinstance(row[9], (datetime.datetime, str)):
        raise AssertionError(f"timestamp_value expected datetime or str, got {{type(row[9]).__name__}}: {{row[9]!r}}")

try:
    cursor = connection.cursor()
    try:
        if smoke_kind == "noop":
            run_noop(cursor)
        elif smoke_kind == "type_matrix":
            run_type_matrix_probe(cursor)
        else:
            raise AssertionError(f"unsupported smoke kind: {{smoke_kind}}")
    finally:
        cursor.close()
finally:
    connection.close()
"#,
        default_type_matrix_table = DEFAULT_TYPE_MATRIX_TABLE
    )
}
