use std::{
    fs::{self, File},
    io,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{Instant, MissedTickBehavior};

const MIB: u64 = 1024 * 1024;
const DEFAULT_STABILITY_SQL: &str = "SELECT WatchID \
    FROM bench_eu.harborsql_clickbench_s3.hits_optimized \
    LIMIT 1";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "one-hour, opt-in memory stability test"]
async fn low_rate_query_workload_has_stable_memory() {
    let config = StabilityConfig::from_env();
    let bind_addr = unused_local_addr();
    let mut server = HarborSqlServer::spawn(bind_addr, &config);

    if let Err(error) = wait_for_healthz(bind_addr, &mut server.child).await {
        let server_log = server.stop_and_read_output();
        panic!("{error}\nHarborSQL log:\n{server_log}");
    }

    let result = run_workload(bind_addr, &mut server.child, &config).await;
    let summary = match result {
        Ok(summary) => summary,
        Err(error) => {
            let server_log = server.stop_and_read_output();
            panic!("{error}\nHarborSQL log:\n{server_log}");
        }
    };

    println!("{summary}");
    assert!(
        summary.ending_growth_bytes <= config.max_growth_bytes,
        "ending RSS growth {} exceeded the configured limit {}; \
         increase HARBORSQL_STABILITY_MAX_GROWTH_MIB only after investigating",
        format_bytes(summary.ending_growth_bytes),
        format_bytes(config.max_growth_bytes),
    );
    assert!(
        summary.peak_bytes <= config.max_rss_bytes,
        "peak RSS {} exceeded the configured limit {}; \
         increase HARBORSQL_STABILITY_MAX_RSS_MIB only after investigating",
        format_bytes(summary.peak_bytes),
        format_bytes(config.max_rss_bytes),
    );
}

struct StabilityConfig {
    duration: Duration,
    request_interval: Duration,
    request_timeout: Duration,
    warmup_requests: usize,
    comparison_window: usize,
    max_growth_bytes: u64,
    max_rss_bytes: u64,
    databricks_host: String,
    bearer_token: String,
    aws_region: String,
    sql: String,
}

impl StabilityConfig {
    fn from_env() -> Self {
        Self {
            duration: Duration::from_secs(env_u64("HARBORSQL_STABILITY_DURATION_SECONDS", 60 * 60)),
            request_interval: Duration::from_millis(env_u64(
                "HARBORSQL_STABILITY_REQUEST_INTERVAL_MILLISECONDS",
                1_000,
            )),
            request_timeout: Duration::from_secs(env_u64(
                "HARBORSQL_STABILITY_REQUEST_TIMEOUT_SECONDS",
                10,
            )),
            warmup_requests: env_usize("HARBORSQL_STABILITY_WARMUP_REQUESTS", 30),
            comparison_window: env_usize("HARBORSQL_STABILITY_COMPARISON_WINDOW", 60),
            max_growth_bytes: env_u64("HARBORSQL_STABILITY_MAX_GROWTH_MIB", 128) * MIB,
            max_rss_bytes: env_u64("HARBORSQL_STABILITY_MAX_RSS_MIB", 1_024) * MIB,
            databricks_host: first_env(&[
                "HARBORSQL_STABILITY_DATABRICKS_HOST",
                "BENCH_EU_DATABRICKS_HOSTNAME",
                "DATABRICKS_HOST",
            ])
            .unwrap_or_else(|| {
                panic!(
                    "set HARBORSQL_STABILITY_DATABRICKS_HOST, \
                     BENCH_EU_DATABRICKS_HOSTNAME, or DATABRICKS_HOST"
                )
            }),
            bearer_token: first_env(&[
                "HARBORSQL_STABILITY_DATABRICKS_TOKEN",
                "DATABRICKS_TOKEN",
                "TEST_CI_DATABRICKS_PAT",
            ])
            .unwrap_or_else(|| {
                panic!(
                    "set HARBORSQL_STABILITY_DATABRICKS_TOKEN, \
                     DATABRICKS_TOKEN, or TEST_CI_DATABRICKS_PAT"
                )
            }),
            aws_region: first_env(&["HARBORSQL_STABILITY_AWS_REGION", "HARBORSQL_AWS_REGION"])
                .unwrap_or_else(|| "eu-west-3".to_string()),
            sql: first_env(&["HARBORSQL_STABILITY_SQL"])
                .unwrap_or_else(|| DEFAULT_STABILITY_SQL.to_string()),
        }
    }
}

async fn run_workload(
    bind_addr: SocketAddr,
    server: &mut Child,
    config: &StabilityConfig,
) -> Result<StabilitySummary, String> {
    let server_pid = server.id();
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;
    let query_url = format!("http://{bind_addr}/api/v1/query");
    let started = Instant::now();
    let deadline = started + config.duration;
    let mut ticker = tokio::time::interval(config.request_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut samples = Vec::new();
    let mut request_count = 0_usize;

    while Instant::now() < deadline {
        ticker.tick().await;
        if Instant::now() >= deadline {
            break;
        }

        if let Some(status) = server
            .try_wait()
            .map_err(|error| format!("failed to poll HarborSQL server: {error}"))?
        {
            return Err(format!(
                "HarborSQL exited during the stability workload with status {status}"
            ));
        }

        request_count += 1;
        let response = client
            .post(&query_url)
            .bearer_auth(&config.bearer_token)
            .json(&json!({ "sql": config.sql }))
            .send()
            .await
            .map_err(|error| format!("query {request_count} failed: {error}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| format!("failed to read query {request_count} response: {error}"))?;
        validate_query_response(request_count, status, &body)?;

        let rss_bytes = resident_set_bytes(server_pid)
            .map_err(|error| format!("failed to sample HarborSQL RSS: {error}"))?;
        samples.push(MemorySample {
            elapsed: started.elapsed(),
            rss_bytes,
        });
    }

    StabilitySummary::from_samples(
        samples,
        request_count,
        config.warmup_requests,
        config.comparison_window,
    )
}

fn validate_query_response(
    request_count: usize,
    status: StatusCode,
    body: &str,
) -> Result<(), String> {
    if !status.is_success() {
        return Err(format!(
            "query {request_count} returned HTTP {status}: {body}"
        ));
    }
    let body: Value = serde_json::from_str(body)
        .map_err(|error| format!("query {request_count} returned invalid JSON: {error}"))?;
    if body.pointer("/result/row_count").and_then(Value::as_u64) != Some(1) {
        return Err(format!(
            "query {request_count} returned an unexpected result: {body}"
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct MemorySample {
    elapsed: Duration,
    rss_bytes: u64,
}

#[derive(Debug)]
struct StabilitySummary {
    request_count: usize,
    elapsed: Duration,
    baseline_bytes: u64,
    ending_bytes: u64,
    ending_growth_bytes: u64,
    minimum_bytes: u64,
    peak_bytes: u64,
    growth_per_hour_bytes: i64,
}

impl StabilitySummary {
    fn from_samples(
        samples: Vec<MemorySample>,
        request_count: usize,
        warmup_requests: usize,
        comparison_window: usize,
    ) -> Result<Self, String> {
        let analyzed = samples.get(warmup_requests..).ok_or_else(|| {
            format!(
                "workload produced {} samples, not enough to skip {warmup_requests} warmup requests",
                samples.len()
            )
        })?;
        if analyzed.len() < 2 {
            return Err(format!(
                "workload needs at least two memory samples after warmup; got {}",
                analyzed.len()
            ));
        }

        let window = comparison_window.min(analyzed.len() / 2);
        if window == 0 {
            return Err("comparison window must be greater than zero".to_string());
        }
        let baseline_bytes = median_rss(&analyzed[..window]);
        let ending_bytes = median_rss(&analyzed[analyzed.len() - window..]);
        let ending_growth_bytes = ending_bytes.saturating_sub(baseline_bytes);
        let minimum_bytes = analyzed
            .iter()
            .map(|sample| sample.rss_bytes)
            .min()
            .expect("analyzed samples are non-empty");
        let peak_bytes = analyzed
            .iter()
            .map(|sample| sample.rss_bytes)
            .max()
            .expect("analyzed samples are non-empty");
        let elapsed = samples
            .last()
            .map(|sample| sample.elapsed)
            .unwrap_or_default();
        let growth_per_hour_bytes =
            linear_growth_per_hour(analyzed).clamp(i64::MIN as f64, i64::MAX as f64) as i64;

        Ok(Self {
            request_count,
            elapsed,
            baseline_bytes,
            ending_bytes,
            ending_growth_bytes,
            minimum_bytes,
            peak_bytes,
            growth_per_hour_bytes,
        })
    }
}

impl std::fmt::Display for StabilitySummary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "HarborSQL stability summary\n\
             requests: {}\n\
             elapsed: {:.1}s\n\
             request rate: {:.3}/s\n\
             RSS baseline median: {}\n\
             RSS ending median: {}\n\
             RSS ending growth: {}\n\
             RSS minimum: {}\n\
             RSS peak: {}\n\
             RSS linear trend: {}/hour",
            self.request_count,
            self.elapsed.as_secs_f64(),
            self.request_count as f64 / self.elapsed.as_secs_f64(),
            format_bytes(self.baseline_bytes),
            format_bytes(self.ending_bytes),
            format_bytes(self.ending_growth_bytes),
            format_bytes(self.minimum_bytes),
            format_bytes(self.peak_bytes),
            format_signed_bytes(self.growth_per_hour_bytes),
        )
    }
}

fn median_rss(samples: &[MemorySample]) -> u64 {
    let mut values = samples
        .iter()
        .map(|sample| sample.rss_bytes)
        .collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn linear_growth_per_hour(samples: &[MemorySample]) -> f64 {
    let count = samples.len() as f64;
    let mean_seconds = samples
        .iter()
        .map(|sample| sample.elapsed.as_secs_f64())
        .sum::<f64>()
        / count;
    let mean_rss = samples
        .iter()
        .map(|sample| sample.rss_bytes as f64)
        .sum::<f64>()
        / count;
    let (covariance, variance) = samples.iter().fold((0.0, 0.0), |acc, sample| {
        let centered_seconds = sample.elapsed.as_secs_f64() - mean_seconds;
        (
            acc.0 + centered_seconds * (sample.rss_bytes as f64 - mean_rss),
            acc.1 + centered_seconds.powi(2),
        )
    });
    if variance == 0.0 {
        0.0
    } else {
        covariance / variance * 60.0 * 60.0
    }
}

fn resident_set_bytes(pid: u32) -> io::Result<u64> {
    #[cfg(target_os = "linux")]
    if let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status"))
        && let Some(kibibytes) = status.lines().find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
    {
        return Ok(kibibytes * 1024);
    }

    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "ps exited with status {}",
            output.status
        )));
    }
    let kibibytes = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(kibibytes * 1024)
}

fn format_bytes(bytes: u64) -> String {
    format!("{:.2} MiB", bytes as f64 / MIB as f64)
}

fn format_signed_bytes(bytes: i64) -> String {
    format!("{:+.2} MiB", bytes as f64 / MIB as f64)
}

fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("invalid {name}={value:?}: {error}")),
        Err(_) => default,
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {name}={value:?}: {error}")),
        Err(_) => default,
    }
}

fn first_env(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

struct HarborSqlServer {
    child: Child,
    log_path: PathBuf,
}

impl HarborSqlServer {
    fn spawn(bind_addr: SocketAddr, config: &StabilityConfig) -> Self {
        let log_path = std::env::temp_dir().join(format!(
            "harborsql-stability-{}-{}.log",
            std::process::id(),
            bind_addr.port()
        ));
        let log_file = File::create(&log_path).expect("failed to create HarborSQL stability log");
        let log_file_for_stderr = log_file
            .try_clone()
            .expect("failed to clone HarborSQL stability log");
        let child = Command::new(env!("CARGO_BIN_EXE_harborsql"))
            .arg("server")
            .env("HARBORSQL_BIND_ADDR", bind_addr.to_string())
            .env("HARBORSQL_DATABRICKS_HOST", &config.databricks_host)
            .env("HARBORSQL_AWS_REGION", &config.aws_region)
            .env("HARBORSQL_DEFAULT_CATALOG", "bench_eu")
            .env("HARBORSQL_DEFAULT_SCHEMA", "harborsql_clickbench_s3")
            .env("RUST_LOG", "harborsql=warn")
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_for_stderr))
            .spawn()
            .expect("failed to start HarborSQL server");
        Self { child, log_path }
    }

    fn stop_and_read_output(&mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        fs::read_to_string(&self.log_path).unwrap_or_else(|error| {
            format!(
                "failed to read HarborSQL stability log {}: {error}",
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

async fn wait_for_healthz(addr: SocketAddr, child: &mut Child) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .map_err(|error| format!("failed to build health-check client: {error}"))?;
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("failed to poll HarborSQL server: {error}"))?
        {
            return Err(format!(
                "HarborSQL exited before its health check succeeded: {status}"
            ));
        }
        if client
            .get(format!("http://{addr}/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::NO_CONTENT)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Err(format!(
        "HarborSQL did not become healthy at http://{addr}/healthz"
    ))
}

#[cfg(test)]
mod tests {
    use super::{MemorySample, StabilitySummary, linear_growth_per_hour, median_rss};
    use std::time::Duration;

    fn sample(seconds: u64, mebibytes: u64) -> MemorySample {
        MemorySample {
            elapsed: Duration::from_secs(seconds),
            rss_bytes: mebibytes * 1024 * 1024,
        }
    }

    #[test]
    fn median_uses_the_middle_sample() {
        let samples = vec![sample(0, 30), sample(1, 10), sample(2, 20)];
        assert_eq!(median_rss(&samples), 20 * 1024 * 1024);
    }

    #[test]
    fn trend_is_reported_per_hour() {
        let samples = vec![sample(0, 10), sample(1_800, 15), sample(3_600, 20)];
        let expected = 10.0 * 1024.0 * 1024.0;
        assert!((linear_growth_per_hour(&samples) - expected).abs() < 0.01);
    }

    #[test]
    fn summary_excludes_warmup_and_compares_window_medians() {
        let samples = vec![
            sample(0, 100),
            sample(1, 20),
            sample(2, 22),
            sample(3, 24),
            sample(4, 28),
            sample(5, 30),
        ];
        let summary = StabilitySummary::from_samples(samples, 6, 1, 2).unwrap();

        assert_eq!(summary.baseline_bytes, 22 * 1024 * 1024);
        assert_eq!(summary.ending_bytes, 30 * 1024 * 1024);
        assert_eq!(summary.ending_growth_bytes, 8 * 1024 * 1024);
        assert_eq!(summary.minimum_bytes, 20 * 1024 * 1024);
        assert_eq!(summary.peak_bytes, 30 * 1024 * 1024);
    }
}
