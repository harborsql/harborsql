use std::{
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use base64::Engine as _;

const DEFAULT_TYPE_MATRIX_TABLE: &str = "bench_eu.harborsql_delta_types.delta_type_matrix";
const DATABRICKS_JDBC_MAVEN_BASE: &str =
    "https://repo1.maven.org/maven2/com/databricks/databricks-jdbc";
const DEFAULT_JDBC_2_6_40_VERSION: &str = "2.6.40";
const DEFAULT_JDBC_3_X_VERSION: &str = "3.3.3";
const JDBC_OAUTH_CLIENT_ID: &str = "harborsql-client-id";
const JDBC_OAUTH_CLIENT_SECRET: &str = "harborsql-client-secret";
const JDBC_OAUTH_ACCESS_TOKEN: &str = "local-token";

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

#[test]
#[ignore = "requires Java and downloads the Databricks JDBC driver if no jar path is configured"]
fn jdbc_databricks_driver_2_6_40_can_execute_probe_query() {
    run_jdbc_smoke(JdbcSmokeTarget {
        label: "2.6.40",
        jar_env: "HARBORSQL_JDBC_SMOKE_2_6_40_JAR",
        version_env: "HARBORSQL_JDBC_SMOKE_2_6_40_VERSION",
        default_version: Some(DEFAULT_JDBC_2_6_40_VERSION.to_string()),
    });
}

#[test]
#[ignore = "requires Java and downloads the Databricks JDBC driver if no jar path is configured"]
fn jdbc_databricks_driver_3_x_can_execute_probe_query() {
    run_jdbc_smoke(JdbcSmokeTarget {
        label: "3.x",
        jar_env: "HARBORSQL_JDBC_SMOKE_3_X_JAR",
        version_env: "HARBORSQL_JDBC_SMOKE_3_X_VERSION",
        default_version: Some(DEFAULT_JDBC_3_X_VERSION.to_string()),
    });
}

#[test]
#[ignore = "requires Java and downloads the Databricks JDBC driver if no jar path is configured"]
fn jdbc_databricks_driver_3_x_can_execute_oauth_m2m_probe_query() {
    run_jdbc_oauth_m2m_smoke(JdbcSmokeTarget {
        label: "3.x OAuth M2M",
        jar_env: "HARBORSQL_JDBC_SMOKE_3_X_JAR",
        version_env: "HARBORSQL_JDBC_SMOKE_3_X_VERSION",
        default_version: Some(DEFAULT_JDBC_3_X_VERSION.to_string()),
    });
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

    let server_stderr = if output.status.success() {
        String::new()
    } else {
        server.stop_and_read_output()
    };

    assert!(
        output.status.success(),
        "Python connector smoke test failed with status {:?}\nstdout:\n{}\nstderr:\n{}\nHarborSQL stderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        server_stderr
    );
}

#[derive(Debug, Clone)]
struct JdbcSmokeTarget<'a> {
    label: &'a str,
    jar_env: &'a str,
    version_env: &'a str,
    default_version: Option<String>,
}

fn run_jdbc_smoke(target: JdbcSmokeTarget<'_>) {
    run_jdbc_smoke_with_url(target, jdbc_smoke_url);
}

fn run_jdbc_oauth_m2m_smoke(target: JdbcSmokeTarget<'_>) {
    let oauth_server = OAuthTokenServer::spawn();
    let token_endpoint = oauth_server.token_endpoint();

    run_jdbc_smoke_with_url(target, |addr| {
        jdbc_oauth_m2m_smoke_url(addr, &token_endpoint)
    });

    let token_request = oauth_server
        .token_request()
        .expect("Databricks JDBC OAuth M2M smoke did not call the token endpoint");
    let token_request_details = format!(
        "request line: {}\nauthorization: {:?}\nbody: {}",
        token_request.request_line, token_request.authorization, token_request.body
    );
    assert!(
        token_request.has_client_credentials_grant,
        "Databricks JDBC OAuth M2M token request did not use client_credentials grant:\n{token_request_details}"
    );
    assert!(
        token_request.has_expected_credentials,
        "Databricks JDBC OAuth M2M token request did not include the configured client id and secret:\n{token_request_details}"
    );
}

fn run_jdbc_smoke_with_url<F>(target: JdbcSmokeTarget<'_>, jdbc_url: F)
where
    F: FnOnce(SocketAddr) -> String,
{
    let Some(driver_jar) = databricks_jdbc_driver_jar(&target) else {
        if require_jdbc_smoke() {
            panic!(
                "Databricks JDBC {} smoke test requires {} or {}",
                target.label, target.jar_env, target.version_env
            );
        }
        eprintln!(
            "skipping Databricks JDBC {} smoke test; no published/default driver version is configured",
            target.label
        );
        return;
    };

    let bind_addr = unused_local_addr();
    let mut server = HarborSqlServer::spawn(bind_addr);
    wait_for_healthz(bind_addr, &mut server.child);

    let classes_dir = compile_jdbc_smoke_client(&driver_jar);
    let url = jdbc_url(bind_addr);
    let output = Command::new(java())
        .arg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .arg("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
        .arg("-DisFakeServiceTest=true")
        .arg("-cp")
        .arg(format!(
            "{}:{}",
            classes_dir.display(),
            driver_jar.display()
        ))
        .arg("JdbcSmoke")
        .arg(&url)
        .env_remove("DATABRICKS_HOST")
        .env_remove("DATABRICKS_TOKEN")
        .env_remove("DATABRICKS_CLIENT_ID")
        .env_remove("DATABRICKS_CLIENT_SECRET")
        .env_remove("DATABRICKS_AUTH_TYPE")
        .env_remove("DATABRICKS_CONFIG_FILE")
        .env_remove("DATABRICKS_CONFIG_PROFILE")
        .output()
        .expect("failed to launch JDBC smoke test");

    let server_stderr = if output.status.success() {
        String::new()
    } else {
        server.stop_and_read_output()
    };

    assert!(
        output.status.success(),
        "Databricks JDBC {} smoke test failed with status {:?}\nurl:\n{}\nstdout:\n{}\nstderr:\n{}\nHarborSQL stderr:\n{}",
        target.label,
        output.status.code(),
        redact_jdbc_url(&url),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        server_stderr
    );
}

#[derive(Debug)]
struct OAuthTokenRequest {
    request_line: String,
    authorization: Option<String>,
    body: String,
    has_client_credentials_grant: bool,
    has_expected_credentials: bool,
}

struct OAuthTokenServer {
    addr: SocketAddr,
    request_rx: mpsc::Receiver<OAuthTokenRequest>,
}

impl OAuthTokenServer {
    fn spawn() -> Self {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("failed to bind local OAuth token server");
        listener
            .set_nonblocking(true)
            .expect("failed to configure local OAuth token server");
        let addr = listener
            .local_addr()
            .expect("failed to read local OAuth token server port");
        let (request_tx, request_rx) = mpsc::channel();

        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(15);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if handle_oauth_request(stream, addr, &request_tx) {
                            return;
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => return,
                }
            }
        });

        Self { addr, request_rx }
    }

    fn token_endpoint(&self) -> String {
        format!("http://{}/oidc/v1/token", self.addr)
    }

    fn token_request(&self) -> Option<OAuthTokenRequest> {
        self.request_rx.recv_timeout(Duration::from_secs(5)).ok()
    }
}

fn handle_oauth_request(
    mut stream: TcpStream,
    addr: SocketAddr,
    request_tx: &mpsc::Sender<OAuthTokenRequest>,
) -> bool {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let Some(request) = read_oauth_token_request(&mut stream) else {
        return true;
    };

    if request
        .request_line
        .starts_with("GET /oidc/.well-known/oauth-authorization-server ")
    {
        write_oauth_json_response(
            &mut stream,
            "200 OK",
            &format!(
                r#"{{"issuer":"http://{addr}","authorization_endpoint":"http://{addr}/oauth/authorize","token_endpoint":"http://{addr}/oidc/v1/token","jwks_uri":"http://{addr}/oidc/jwks","response_types_supported":["code"],"grant_types_supported":["client_credentials"],"token_endpoint_auth_methods_supported":["client_secret_basic","client_secret_post"]}}"#
            ),
        );
        return false;
    }

    let success = request.has_expected_credentials
        && request.request_line.starts_with("POST /oidc/v1/token ");
    let _ = request_tx.send(request);

    let (status, body) = if success {
        (
            "200 OK",
            format!(
                r#"{{"access_token":"{JDBC_OAUTH_ACCESS_TOKEN}","token_type":"Bearer","expires_in":3600}}"#
            ),
        )
    } else {
        (
            "401 Unauthorized",
            r#"{"error":"invalid_client"}"#.to_string(),
        )
    };
    write_oauth_json_response(&mut stream, status, &body);
    true
}

fn write_oauth_json_response(stream: &mut TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

fn read_oauth_token_request(stream: &mut TcpStream) -> Option<OAuthTokenRequest> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(header_end) = find_header_end(&bytes) {
            break header_end;
        }
    };

    let header_text = String::from_utf8_lossy(&bytes[..header_end]).to_string();
    let content_length = content_length(&header_text).unwrap_or(0);
    let body_start = header_end + 4;
    while bytes.len() < body_start + content_length {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }

    let body_end = bytes.len().min(body_start + content_length);
    let body = String::from_utf8_lossy(&bytes[body_start..body_end]).to_string();
    let request_line = header_text.lines().next().unwrap_or("").to_string();
    let authorization = header_value_from_raw_headers(&header_text, "authorization");
    let has_client_credentials_grant =
        form_value(&body, "grant_type").is_some_and(|value| value == "client_credentials");
    let has_expected_credentials = body_has_expected_oauth_credentials(&body)
        || basic_auth_has_expected_oauth_credentials(&authorization);

    Some(OAuthTokenRequest {
        request_line,
        authorization,
        body,
        has_client_credentials_grant,
        has_expected_credentials,
    })
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &str) -> Option<usize> {
    header_value_from_raw_headers(headers, "content-length")?
        .parse()
        .ok()
}

fn header_value_from_raw_headers(headers: &str, name: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        if key.trim().eq_ignore_ascii_case(name) {
            Some(value.trim().to_string())
        } else {
            None
        }
    })
}

fn body_has_expected_oauth_credentials(body: &str) -> bool {
    form_value(body, "client_id").is_some_and(|value| value == JDBC_OAUTH_CLIENT_ID)
        && form_value(body, "client_secret").is_some_and(|value| value == JDBC_OAUTH_CLIENT_SECRET)
}

fn basic_auth_has_expected_oauth_credentials(authorization: &Option<String>) -> bool {
    let Some(authorization) = authorization else {
        return false;
    };
    let Some((scheme, encoded)) = authorization.trim().split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("basic") {
        return false;
    }
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded.trim()) else {
        return false;
    };
    let Ok(credentials) = String::from_utf8(decoded) else {
        return false;
    };
    credentials == format!("{JDBC_OAUTH_CLIENT_ID}:{JDBC_OAUTH_CLIENT_SECRET}")
}

fn form_value(body: &str, key: &str) -> Option<String> {
    body.split('&').find_map(|part| {
        let (raw_key, raw_value) = part.split_once('=')?;
        let decoded_key = urlencoding::decode(raw_key).ok()?;
        if decoded_key == key {
            Some(urlencoding::decode(raw_value).ok()?.into_owned())
        } else {
            None
        }
    })
}

struct HarborSqlServer {
    child: Child,
    log_path: PathBuf,
}

impl HarborSqlServer {
    fn spawn(bind_addr: SocketAddr) -> Self {
        let log_path = std::env::temp_dir().join(format!(
            "harborsql-connector-smoke-{}-{}.log",
            std::process::id(),
            bind_addr.port()
        ));
        let log_file = File::create(&log_path).expect("failed to create HarborSQL smoke log file");
        let log_file_for_stderr = log_file
            .try_clone()
            .expect("failed to clone HarborSQL smoke log file");

        let child = Command::new(env!("CARGO_BIN_EXE_harborsql"))
            .arg("server")
            .env("HARBORSQL_BIND_ADDR", bind_addr.to_string())
            .env("HARBORSQL_DATABRICKS_HOST", databricks_host())
            .env("HARBORSQL_DEFAULT_CATALOG", configured_connector_catalog())
            .env("HARBORSQL_DEFAULT_SCHEMA", configured_connector_schema())
            .env("RUST_LOG", "harborsql=debug")
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_for_stderr))
            .spawn()
            .expect("failed to start HarborSQL server");

        Self { child, log_path }
    }

    fn stop_and_read_output(&mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();

        fs::read_to_string(&self.log_path).unwrap_or_else(|err| {
            format!(
                "failed to read HarborSQL smoke log {}: {err}",
                self.log_path.display()
            )
        })
    }
}

impl Drop for HarborSqlServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_file(&self.log_path);
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

fn java() -> String {
    std::env::var("HARBORSQL_JDBC_SMOKE_JAVA").unwrap_or_else(|_| "java".to_string())
}

fn javac() -> String {
    std::env::var("HARBORSQL_JDBC_SMOKE_JAVAC").unwrap_or_else(|_| "javac".to_string())
}

fn databricks_jdbc_driver_jar(target: &JdbcSmokeTarget<'_>) -> Option<PathBuf> {
    if let Some(path) = optional_env(target.jar_env) {
        return Some(PathBuf::from(path));
    }

    let version = optional_env(target.version_env).or_else(|| target.default_version.clone())?;
    Some(download_databricks_jdbc_driver(&version))
}

fn download_databricks_jdbc_driver(version: &str) -> PathBuf {
    let jar_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/jdbc-smoke-jars");
    fs::create_dir_all(&jar_dir).expect("failed to create JDBC smoke jar cache directory");
    let jar_path = jar_dir.join(format!("databricks-jdbc-{version}.jar"));
    if jar_path.exists()
        && fs::metadata(&jar_path)
            .expect("failed to read cached Databricks JDBC driver metadata")
            .len()
            > 0
    {
        return jar_path;
    }

    let url = format!("{DATABRICKS_JDBC_MAVEN_BASE}/{version}/databricks-jdbc-{version}.jar");
    let output = Command::new("curl")
        .args(["-fsSL", "-o"])
        .arg(&jar_path)
        .arg(&url)
        .output()
        .expect("failed to launch curl to download Databricks JDBC driver");
    assert!(
        output.status.success(),
        "failed to download Databricks JDBC driver {version} from {url}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    jar_path
}

fn compile_jdbc_smoke_client(driver_jar: &Path) -> PathBuf {
    let work_dir = std::env::temp_dir().join(format!(
        "harborsql-jdbc-smoke-{}-{}",
        std::process::id(),
        stable_path_hash(driver_jar)
    ));
    let classes_dir = work_dir.join("classes");
    fs::create_dir_all(&classes_dir).expect("failed to create JDBC smoke class directory");
    let source_path = work_dir.join("JdbcSmoke.java");
    fs::write(&source_path, jdbc_smoke_java_source()).expect("failed to write JDBC smoke source");

    let output = Command::new(javac())
        .arg("-cp")
        .arg(driver_jar)
        .arg("-d")
        .arg(&classes_dir)
        .arg(&source_path)
        .output()
        .expect("failed to launch javac for JDBC smoke test");
    assert!(
        output.status.success(),
        "failed to compile JDBC smoke client\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    classes_dir
}

fn stable_path_hash(path: &Path) -> u64 {
    path.display()
        .to_string()
        .bytes()
        .fold(0_u64, |hash, byte| {
            hash.wrapping_mul(109).wrapping_add(byte as u64)
        })
}

fn jdbc_smoke_url(addr: SocketAddr) -> String {
    format!(
        "jdbc:databricks://{}:{}/default;\
transportMode=http;\
ssl=0;\
AuthMech=3;\
UID=token;\
PWD=local-token;\
httpPath=/sql/1.0/warehouses/local-smoke;\
UseThriftClient=1;\
EnableArrow=0;\
EnableQueryResultDownload=0;\
EnableDirectResults=0;\
EnableSQLExecDirectResults=0;\
RowsFetchedPerBlock=1;\
UseNativeQuery=1;\
ConnCatalog={};\
ConnSchema={}",
        addr.ip(),
        addr.port(),
        configured_connector_catalog(),
        configured_connector_schema()
    )
}

fn jdbc_oauth_m2m_smoke_url(addr: SocketAddr, token_endpoint: &str) -> String {
    let discovery_endpoint = token_endpoint.replace(
        "/oidc/v1/token",
        "/oidc/.well-known/oauth-authorization-server",
    );
    format!(
        "jdbc:databricks://{}:{}/default;\
transportMode=http;\
ssl=0;\
AuthMech=11;\
Auth_Flow=1;\
OAuth2ClientId={JDBC_OAUTH_CLIENT_ID};\
OAuth2Secret={JDBC_OAUTH_CLIENT_SECRET};\
OAuth2TokenEndpoint={};\
OAuth2ConnAuthTokenEndpoint={};\
OAuthDiscoveryURL={};\
OIDCDiscoveryEndpoint={};\
OAuthDiscoveryMode=1;\
EnableOIDCDiscovery=1;\
httpPath=/sql/1.0/warehouses/local-smoke;\
UseThriftClient=1;\
EnableArrow=0;\
EnableQueryResultDownload=0;\
EnableDirectResults=0;\
EnableSQLExecDirectResults=0;\
RowsFetchedPerBlock=1;\
UseNativeQuery=1;\
ConnCatalog={};\
ConnSchema={}",
        addr.ip(),
        addr.port(),
        token_endpoint,
        token_endpoint,
        discovery_endpoint,
        discovery_endpoint,
        configured_connector_catalog(),
        configured_connector_schema()
    )
}

fn redact_jdbc_url(url: &str) -> String {
    url.split(';')
        .map(|part| {
            if let Some((key, _value)) = part.split_once('=')
                && matches!(
                    key.to_ascii_lowercase().as_str(),
                    "pwd" | "password" | "oauth2secret" | "auth_accesstoken"
                )
            {
                format!("{key}=<redacted>")
            } else {
                part.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn require_jdbc_smoke() -> bool {
    optional_env("HARBORSQL_JDBC_SMOKE_REQUIRE").is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn jdbc_smoke_java_source() -> &'static str {
    r#"
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public final class JdbcSmoke {
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("expected JDBC URL argument");
    }
    Class.forName("com.databricks.client.jdbc.Driver");
    try (Connection connection = DriverManager.getConnection(args[0]);
         Statement statement = connection.createStatement()) {
      statement.setFetchSize(1);
      try (ResultSet resultSet =
          statement.executeQuery("SELECT CAST(1 AS INT) AS id, 'harbor' AS label")) {
        ResultSetMetaData metadata = resultSet.getMetaData();
        require(metadata.getColumnCount() == 2, "expected two result columns");
        require("id".equalsIgnoreCase(metadata.getColumnLabel(1)), "unexpected first column label");
        require("label".equalsIgnoreCase(metadata.getColumnLabel(2)), "unexpected second column label");
        require(resultSet.next(), "expected one result row");
        require(resultSet.getInt(1) == 1, "unexpected id value");
        require("harbor".equals(resultSet.getString(2)), "unexpected label value");
        require(!resultSet.next(), "expected exactly one result row");
      }
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
"#
}

fn databricks_host() -> String {
    configured_databricks_host().unwrap_or_else(|| "https://example.com".to_string())
}

fn configured_databricks_host() -> Option<String> {
    optional_env("HARBORSQL_DATABRICKS_HOST")
        .or_else(|| optional_env("DATABRICKS_HOST"))
        .or_else(|| optional_env("BENCH_EU_DATABRICKS_HOSTNAME"))
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

def disable_local_token_federation():
    try:
        from databricks.sql.auth.token_federation import TokenFederationProvider
        TokenFederationProvider._should_exchange_token = lambda self, access_token: False
    except Exception:
        pass

if auth_mode == "local":
    connect_kwargs["access_token"] = "local-token"
elif auth_mode == "pat":
    if not pat_token:
        raise AssertionError("PAT smoke test requires DATABRICKS_TOKEN or TEST_CI_DATABRICKS_PAT")
    connect_kwargs["access_token"] = pat_token
elif auth_mode == "oauth" or client_id or client_secret:
    disable_local_token_federation()
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
