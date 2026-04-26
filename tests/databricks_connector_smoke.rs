use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[test]
#[ignore = "requires Python with databricks-sql-connector installed"]
fn python_databricks_sql_connector_can_execute_noop_statement() {
    let bind_addr = unused_local_addr();
    let mut server = HarborSqlServer::spawn(bind_addr);
    wait_for_healthz(bind_addr, &mut server.child);

    let mut command = Command::new(python());
    command.arg("-c").arg(connector_smoke_script(bind_addr));
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
    if let Some(token) = configured_pat_token() {
        command.env("DATABRICKS_TOKEN", token);
    }
    if let Some(auth_mode) = optional_env("HARBORSQL_CONNECTOR_SMOKE_AUTH") {
        command.env("HARBORSQL_CONNECTOR_SMOKE_AUTH", auth_mode);
    }

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
            .env("HARBORSQL_DEFAULT_CATALOG", "workspace")
            .env("HARBORSQL_DEFAULT_SCHEMA", "default")
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

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn connector_smoke_script(addr: SocketAddr) -> String {
    format!(
        r#"
from databricks import sql
import os

uri = "http://{addr}"
connection_uri = uri + "/sql/1.0/warehouses/local-smoke"
databricks_host = os.environ.get("DATABRICKS_HOST")
client_id = os.environ.get("DATABRICKS_CLIENT_ID")
client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
account_id = os.environ.get("DATABRICKS_ACCOUNT_ID")
pat_token = os.environ.get("DATABRICKS_TOKEN")
auth_mode = os.environ.get("HARBORSQL_CONNECTOR_SMOKE_AUTH", "auto").lower()

connect_kwargs = dict(
    server_hostname=uri,
    http_path="/sql/1.0/warehouses/local-smoke",
    catalog="workspace",
    schema="default",
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

try:
    cursor = connection.cursor()
    try:
        cursor.execute("SET use_cached_result = false")
        rows = cursor.fetchall()
        query_id = getattr(cursor, "query_id", None)

        if rows != []:
            raise AssertionError(f"expected no rows, got {{rows!r}}")
        if not query_id:
            raise AssertionError("expected connector cursor to expose a query_id")
    finally:
        cursor.close()
finally:
    connection.close()
"#
    )
}
