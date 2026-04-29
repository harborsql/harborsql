use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{Mutex, MutexGuard, OnceLock},
    time::Duration,
};

use sha2::{Digest, Sha256};

use crate::error::redact_sensitive;

static OBSERVABILITY: OnceLock<Observability> = OnceLock::new();

pub fn init(unsafe_log_sql: bool) {
    let _ = OBSERVABILITY.set(Observability::new(unsafe_log_sql));
}

pub fn get() -> &'static Observability {
    OBSERVABILITY.get_or_init(|| Observability::new(false))
}

pub struct Observability {
    unsafe_log_sql: bool,
    metrics: Metrics,
}

impl Observability {
    fn new(unsafe_log_sql: bool) -> Self {
        Self {
            unsafe_log_sql,
            metrics: Metrics::default(),
        }
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn sql_observation(&self, sql: &str) -> SqlObservation {
        SqlObservation {
            hash: stable_hash(sql),
            len: sql.len(),
            text: self.unsafe_log_sql.then(|| redact_sensitive(sql)),
        }
    }
}

pub struct SqlObservation {
    pub hash: String,
    pub len: usize,
    pub text: Option<String>,
}

#[derive(Default)]
pub struct Metrics {
    counters: Mutex<BTreeMap<&'static str, u64>>,
    gauges: Mutex<BTreeMap<&'static str, i64>>,
    timings: Mutex<BTreeMap<&'static str, TimingStats>>,
    http: Mutex<BTreeMap<HttpKey, RequestStats>>,
    thrift: Mutex<BTreeMap<ThriftKey, RequestStats>>,
}

impl Metrics {
    pub fn increment(&self, name: &'static str) {
        self.add(name, 1);
    }

    pub fn add(&self, name: &'static str, value: u64) {
        *lock(&self.counters).entry(name).or_default() += value;
    }

    pub fn set_gauge(&self, name: &'static str, value: i64) {
        lock(&self.gauges).insert(name, value);
    }

    pub fn observe_duration(&self, stage: &'static str, duration: Duration) {
        let mut timings = lock(&self.timings);
        timings
            .entry(stage)
            .or_default()
            .observe(duration.as_secs_f64() * 1000.0);
    }

    pub fn observe_http(&self, method: &str, route: &str, status: u16, duration: Duration) {
        let key = HttpKey {
            method: method.to_string(),
            route: route.to_string(),
            status,
        };
        lock(&self.http)
            .entry(key)
            .or_default()
            .observe(duration.as_secs_f64() * 1000.0);
    }

    pub fn observe_thrift(&self, method: &str, status: &'static str, duration: Duration) {
        let key = ThriftKey {
            method: method.to_string(),
            status,
        };
        lock(&self.thrift)
            .entry(key)
            .or_default()
            .observe(duration.as_secs_f64() * 1000.0);
    }

    pub fn render_prometheus(&self) -> String {
        let mut output = String::new();

        let counters = lock(&self.counters);
        for (name, value) in counters.iter() {
            write_help_type(&mut output, name, "counter");
            let _ = writeln!(output, "{name} {value}");
        }
        drop(counters);

        let gauges = lock(&self.gauges);
        for (name, value) in gauges.iter() {
            write_help_type(&mut output, name, "gauge");
            let _ = writeln!(output, "{name} {value}");
        }
        drop(gauges);

        let timings = lock(&self.timings);
        if !timings.is_empty() {
            output.push_str(
                "# HELP harborsql_stage_duration_ms Query-stage duration in milliseconds\n",
            );
            output.push_str("# TYPE harborsql_stage_duration_ms summary\n");
        }
        for (stage, stats) in timings.iter() {
            let stage = escape_label(stage);
            let _ = writeln!(
                output,
                "harborsql_stage_duration_ms_count{{stage=\"{stage}\"}} {}",
                stats.count
            );
            let _ = writeln!(
                output,
                "harborsql_stage_duration_ms_sum{{stage=\"{stage}\"}} {:.3}",
                stats.sum_ms
            );
        }
        drop(timings);

        let http = lock(&self.http);
        if !http.is_empty() {
            output.push_str(
                "# HELP harborsql_http_requests_total HTTP requests by method, route, and status\n",
            );
            output.push_str("# TYPE harborsql_http_requests_total counter\n");
            output.push_str(
                "# HELP harborsql_http_request_duration_ms HTTP request duration in milliseconds\n",
            );
            output.push_str("# TYPE harborsql_http_request_duration_ms summary\n");
        }
        for (key, stats) in http.iter() {
            let method = escape_label(&key.method);
            let route = escape_label(&key.route);
            let _ = writeln!(
                output,
                "harborsql_http_requests_total{{method=\"{method}\",route=\"{route}\",status=\"{}\"}} {}",
                key.status, stats.count
            );
            let _ = writeln!(
                output,
                "harborsql_http_request_duration_ms_count{{method=\"{method}\",route=\"{route}\",status=\"{}\"}} {}",
                key.status, stats.count
            );
            let _ = writeln!(
                output,
                "harborsql_http_request_duration_ms_sum{{method=\"{method}\",route=\"{route}\",status=\"{}\"}} {:.3}",
                key.status, stats.sum_ms
            );
        }
        drop(http);

        let thrift = lock(&self.thrift);
        if !thrift.is_empty() {
            output
                .push_str("# HELP harborsql_thrift_rpcs_total Thrift RPCs by method and status\n");
            output.push_str("# TYPE harborsql_thrift_rpcs_total counter\n");
            output.push_str(
                "# HELP harborsql_thrift_rpc_duration_ms Thrift RPC duration in milliseconds\n",
            );
            output.push_str("# TYPE harborsql_thrift_rpc_duration_ms summary\n");
        }
        for (key, stats) in thrift.iter() {
            let method = escape_label(&key.method);
            let status = escape_label(key.status);
            let _ = writeln!(
                output,
                "harborsql_thrift_rpcs_total{{method=\"{method}\",status=\"{status}\"}} {}",
                stats.count
            );
            let _ = writeln!(
                output,
                "harborsql_thrift_rpc_duration_ms_count{{method=\"{method}\",status=\"{status}\"}} {}",
                stats.count
            );
            let _ = writeln!(
                output,
                "harborsql_thrift_rpc_duration_ms_sum{{method=\"{method}\",status=\"{status}\"}} {:.3}",
                stats.sum_ms
            );
        }

        output
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct HttpKey {
    method: String,
    route: String,
    status: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ThriftKey {
    method: String,
    status: &'static str,
}

#[derive(Default)]
struct RequestStats {
    count: u64,
    sum_ms: f64,
}

impl RequestStats {
    fn observe(&mut self, duration_ms: f64) {
        self.count += 1;
        self.sum_ms += duration_ms;
    }
}

#[derive(Default)]
struct TimingStats {
    count: u64,
    sum_ms: f64,
}

impl TimingStats {
    fn observe(&mut self, duration_ms: f64) {
        self.count += 1;
        self.sum_ms += duration_ms;
    }
}

pub fn stable_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut output = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn write_help_type(output: &mut String, name: &str, metric_type: &str) {
    let _ = writeln!(output, "# HELP {name} HarborSQL metric {name}");
    let _ = writeln!(output, "# TYPE {name} {metric_type}");
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("observability lock should not be poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_observation_hides_sql_by_default() {
        let observability = Observability::new(false);
        let observed = observability.sql_observation("SELECT * FROM secret_table");

        assert_eq!(observed.len, 26);
        assert!(observed.text.is_none());
        assert_eq!(observed.hash.len(), 16);
    }

    #[test]
    fn sql_observation_redacts_sql_when_enabled() {
        let observability = Observability::new(true);
        let observed = observability.sql_observation("SELECT * FROM delta.`s3://bucket/private`");
        let text = observed.text.unwrap();

        assert!(text.contains("[REDACTED_PATH]"));
        assert!(!text.contains("bucket/private"));
    }

    #[test]
    fn prometheus_output_escapes_labels() {
        let metrics = Metrics::default();
        metrics.observe_http("GET", "/path/\"quoted\"", 200, Duration::from_millis(5));

        let output = metrics.render_prometheus();

        assert!(output.contains("route=\"/path/\\\"quoted\\\"\""));
    }
}
