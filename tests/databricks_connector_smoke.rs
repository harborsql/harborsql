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

    let output = Command::new(python())
        .arg("-c")
        .arg(connector_smoke_script(bind_addr))
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
            .env("HARBORSQL_DATABRICKS_HOST", "https://example.com")
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

fn connector_smoke_script(addr: SocketAddr) -> String {
    format!(
        r#"
from databricks import sql

uri = "http://{addr}"
connection_uri = uri + "/sql/1.0/warehouses/local-smoke"

connection = sql.connect(
    server_hostname=uri,
    http_path="/sql/1.0/warehouses/local-smoke",
    access_token="local-token",
    catalog="workspace",
    schema="default",
    _connection_uri=connection_uri,
    use_cloud_fetch=False,
)

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
